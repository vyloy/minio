package cmd

import (
	"bytes"
	"context"
	"fmt"
	"github.com/minio/minio/internal/event"
	log "github.com/minio/minio/internal/logger"
	"gorm.io/datatypes"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	magicSuffix = ".$^.^/"
)

var (
	globalDB  atomic.Pointer[gorm.DB]
	eventChan = make(chan eventArgs, 32)
	waitSync  sync.WaitGroup
	async     bool
	likeOnly  = true
)

type dbObject struct {
	ObjectInfo `gorm:"embedded"`
	Path       datatypes.JSON `gorm:"index"`
}

func init() {
	p := os.Getenv("LIST_DATA_PATH")
	if len(p) < 1 {
		return
	}
	r := strings.ToLower(os.Getenv("LIST_DATA_RECREATE")) == "true"
	if r {
		waitSync.Add(1)
		err := os.Remove(p)
		if err != nil {
			log.Info("failed to rm sqlite db file: %s", err.Error())
		}
	}
	levelValue := os.Getenv("LIST_DATA_ORM_LOG_LEVEL")
	level := logger.Error
	switch strings.ToLower(levelValue) {
	case "info":
		level = logger.Info
	case "warn":
		level = logger.Warn
	case "error":
		level = logger.Error
	case "silent":
		level = logger.Silent
	}
	db, err := gorm.Open(sqlite.Open(p), &gorm.Config{
		Logger: logger.Default.LogMode(level),
	})
	if err != nil {
		log.Fatal(err, "failed to open sqlite db file")
	}

	globalDB.Store(db)
	if strings.ToLower(os.Getenv("LIST_DATA_USE_JSON_SQL")) == "true" {
		likeOnly = false
	}

	if r {
		go syncData(db)
	}

	async = strings.ToLower(os.Getenv("LIST_DATA_ASYNC_MODE")) == "true"
	if async {
		go eventReceiver()
	}
}

func syncData(db *gorm.DB) {
	s := time.Now()
	defer func() {
		waitSync.Done()
		log.Info("sync db data done, cost: %s", time.Now().Sub(s))
	}()
	api := getGlobalObjectAPI()
	ctx := context.Background()
	buckets, err := api.ListBuckets(ctx)
	if err != nil {
		log.Fatal(err, "failed to list buckets: %s", err.Error())
	}
	var wg sync.WaitGroup
	for _, bucket := range buckets {
		wg.Add(1)
		objInfoCh := make(chan ObjectInfo, 32)
		go syncReceiver(ctx, db, objInfoCh, &wg)
		err = db.Table(bucket.Name).AutoMigrate(&dbObject{})
		if err != nil {
			log.Fatal(err, "failed to create table for bucket(%s): %s", bucket.Name, err.Error())
		}
		if api, ok := api.(*dbObjectLayer); ok {
			err = api.walk(ctx, bucket.Name, "", objInfoCh, ObjectOptions{})
			if err != nil {
				log.Fatal(err, "failed to walk bucket(%s): %s", bucket.Name, err.Error())
			}
		}
	}
	wg.Wait()
}

func syncReceiver(ctx context.Context, db *gorm.DB, ch chan ObjectInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case o, ok := <-ch:
			if !ok {
				return
			}
			if len(o.Name) <= 0 {
				continue
			}
			if len(o.Name) > 245 && strings.HasSuffix(o.Name, magicSuffix) {
				continue
			}
			if o.UserDefined == nil {
				o.UserDefined = map[string]string{}
			}
			o.Name = recoverLongObjectName(o.Name)
			tx := db.Table(o.Bucket).Clauses(clause.OnConflict{
				UpdateAll: true,
			}).Create(&dbObject{
				ObjectInfo: o,
				Path:       convertName2Dir(o.Name),
			})
			if tx.Error != nil {
				log.Fatal(tx.Error, "failed to handle %#v in sync: %s", o, tx.Error.Error())
			}
		case <-ctx.Done():
			return
		}
	}
}

func convertName2Dir(name string) []byte {
	return convertName2Path(name, 1)
}

func convertName2Path(name string, n int) []byte {
	if len(name) < 1 {
		return nil
	}
	ns := strings.Split(name, slashSeparator)
	if len(ns[len(ns)-1]) == 0 {
		ns = ns[:len(ns)-1]
	}
	if len(ns)-n == 0 {
		return nil
	}
	var b bytes.Buffer
	b.WriteByte('[')
	for i := 0; i < len(ns)-n; i++ {
		d := ns[i]
		b.WriteByte('"')
		b.WriteString(d)
		if i == len(ns)-n-1 {
			b.WriteByte('"')
			break
		}
		b.Write([]byte{'"', ','})
	}
	b.WriteByte(']')
	return b.Bytes()
}

func (l *dbObjectLayer) walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo, opts ObjectOptions) error {
	z := mustCast2ErasureServerPools(l.ObjectLayer)
	if err := checkListObjsArgs(ctx, bucket, prefix, "", z); err != nil {
		close(results)
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer func() {
			cancel()
			close(results)
		}()

		for _, erasureSet := range z.serverPools {
			var wg sync.WaitGroup
			for _, set := range erasureSet.sets {
				wg.Add(1)
				go func() {
					defer wg.Done()

					disks, _ := set.getOnlineDisksWithHealing()
					if len(disks) == 0 {
						cancel()
						return
					}

					loadEntry := func(entry metaCacheEntry) {
						fivs, err := entry.fileInfoVersions(bucket)
						if err != nil {
							cancel()
							return
						}
						//for i := len(fivs.Versions) - 1; i >= 0; i-- {
						//	version := fivs.Versions[i]
						//	results <- version.ToObjectInfo(bucket, version.Name)
						//}
						for _, version := range fivs.Versions {
							results <- version.ToObjectInfo(bucket, version.Name)
						}
					}

					resolver := metadataResolutionParams{
						dirQuorum: 1,
						objQuorum: 1,
						bucket:    bucket,
					}

					path := baseDirFromPrefix(prefix)
					filterPrefix := strings.Trim(strings.TrimPrefix(prefix, path), slashSeparator)
					if path == prefix {
						filterPrefix = ""
					}

					lopts := listPathRawOptions{
						disks:          disks[:1],
						fallbackDisks:  disks[1:],
						bucket:         bucket,
						path:           path,
						filterPrefix:   filterPrefix,
						recursive:      true,
						forwardTo:      "",
						minDisks:       1,
						reportNotFound: false,
						agreed:         loadEntry,
						partial: func(entries metaCacheEntries, nAgreed int, errs []error) {
							entry, ok := entries.resolve(&resolver)
							if !ok {
								entry, _ = entries.firstFound()
							}

							loadEntry(*entry)
						},
						finished: nil,
					}

					if err := listPathRaw(ctx, lopts); err != nil {
						log.LogIf(ctx, fmt.Errorf("listPathRaw returned %w: opts(%#v)", err, lopts))
						return
					}
				}()
			}
			wg.Wait()
		}
	}()

	return nil
}

func processEvent(e eventArgs) {
	log.Info("processEvent event(%s): %#v\n", e.EventName, e)
	if globalDB.Load() == nil {
		return
	}
	if !async {
		//defer func() {
		//	if e := recover(); e != nil {
		//		log.Error("processEvent recovered from: %v", e)
		//	}
		//}()
		log.Info("processEvent event(%s): %#v", e.EventName, e)
		var err error
		for i := 0; i < 3; i++ {
			err = handleEvent(e)
			if err == nil {
				break
			}
		}
		if err != nil {
			dbFailed(err)
		}
		return
	}
	eventChan <- e
}

func dbFailed(err error) {
	globalDB.Store(nil)
	panic(err)
}

func eventReceiver() {
	defer func() {
		if e := recover(); e != nil {
			log.Error("eventReceiver recovered from: %v", e)
		}
		log.Info("eventReceiver exited")
	}()
	for e := range eventChan {
		log.Info("received event(%s): %#v", e.EventName, e)
		var err error
		for i := 0; i < 3; i++ {
			err = handleEvent(e)
			if err == nil {
				break
			}
		}
		if err != nil {
			dbFailed(err)
			return
		}
	}
}

func handleEvent(e eventArgs) error {
	if e.Object.UserDefined == nil {
		e.Object.UserDefined = map[string]string{}
	}
	switch e.EventName {
	case event.ObjectRemovedDeleteMarkerCreated:
		e.Object.Name = recoverLongObjectName(e.Object.Name)
		if err := globalDB.Load().Transaction(func(tx *gorm.DB) error {
			var n int
			err := tx.Table(e.BucketName).Where("name = ?", e.Object.Name).Select("COUNT(name)").Row().Scan(&n)
			if err != nil {
				log.Error("failed to handle %s in eventReceiver: %s", e.EventName, err)
				return err
			}
			if n <= 0 {
				return nil
			}

			x := tx.Table(e.Object.Bucket).Where("name = ?", e.Object.Name).Update("is_latest", false)
			if x.Error != nil {
				log.Error("failed to handle %s in eventReceiver: %s", e.EventName, x.Error.Error())
				return x.Error
			}

			value := &dbObject{
				ObjectInfo: e.Object,
				Path:       convertName2Dir(e.Object.Name),
			}
			if value.ModTime.IsZero() {
				value.ModTime = time.Now()
			}
			x = tx.Table(e.BucketName).Create(value)
			return x.Error
		}); err != nil {
			log.Error("failed to handle %s in eventReceiver: %s", e.EventName, err.Error())
			return err
		}
	case event.ObjectCreatedCompleteMultipartUpload,
		event.ObjectCreatedCopy,
		event.ObjectCreatedPost,
		event.ObjectCreatedPut,
		event.ObjectCreatedPutRetention,
		event.ObjectCreatedPutLegalHold,
		event.ObjectCreatedPutTagging,
		event.ObjectCreatedDeleteTagging:
		e.Object.Name = recoverLongObjectName(e.Object.Name)
		dir := e.Object.Name
		if err := globalDB.Load().Transaction(func(tx *gorm.DB) error {
			for {
				dir = filepath.Dir(dir)
				if dir == "." || dir == slashSeparator {
					break
				}
				do := ObjectInfo{
					Name:        dir,
					IsDir:       true,
					UserDefined: map[string]string{},
				}
				if !strings.HasSuffix(dir, slashSeparator) {
					do.Name += slashSeparator
				}
				x := tx.Table(e.Object.Bucket).Clauses(clause.OnConflict{
					DoNothing: true,
				}).Create(&dbObject{
					ObjectInfo: do,
					Path:       convertName2Dir(do.Name),
				})
				if x.Error != nil {
					log.Error("failed to handle %s in eventReceiver: %s", e.EventName, x.Error.Error())
					return x.Error
				}
			}
			x := tx.Table(e.Object.Bucket).Where("name = ?", e.Object.Name).Update("is_latest", false)
			if x.Error != nil {
				log.Error("failed to handle %s in eventReceiver: %s", e.EventName, x.Error.Error())
				return x.Error
			}

			x = tx.Table(e.Object.Bucket).Clauses(clause.OnConflict{
				UpdateAll: true,
			}).Create(&dbObject{
				ObjectInfo: e.Object,
				Path:       convertName2Dir(e.Object.Name),
			})
			return x.Error
		}); err != nil {
			log.Error("failed to handle %s in eventReceiver: %s", e.EventName, err.Error())
			return err
		}
	case event.ObjectRemovedDelete:
		e.Object.Name = recoverLongObjectName(e.Object.Name)
		value := &dbObject{
			ObjectInfo: ObjectInfo{
				Name:      e.Object.Name,
				VersionID: e.Object.VersionID,
			},
		}
		db := globalDB.Load()
		if err := db.Transaction(func(tx *gorm.DB) error {
			x := tx.Table(e.BucketName).Delete(value)
			if x.Error != nil {
				log.Error("failed to handle %s in eventReceiver: %s", e.EventName, x.Error.Error())
				return x.Error
			}
			if tx.RowsAffected == 1 {
				dir := filepath.Dir(e.Object.Name)
				if dir == "." || dir == slashSeparator {
					return nil
				}
				ns := strings.Split(strings.TrimSuffix(dir, slashSeparator), slashSeparator)
				var args []any
				if strings.HasSuffix(dir, slashSeparator) {
					args = append(args, dir)
				} else {
					args = append(args, dir+slashSeparator)
				}
				args = append(args, len(ns))
				sub := fmt.Sprintf("SELECT count(name) FROM `%s` WHERE json_array_length(path) >= ?", e.BucketName)
				for i, n := range ns {
					sub += " AND path ->> ? = ?"
					args = append(args, i)
					args = append(args, n)
				}
				x = tx.Exec(fmt.Sprintf("DELETE FROM `%s` WHERE name = ? AND 0 = (%s)", e.BucketName, sub), args...)
				if x.Error != nil {
					log.Error("failed to handle %s in eventReceiver: %s", e.EventName, x.Error.Error())
					return x.Error
				}
			}
			return nil
		}); err != nil {
			log.Error("failed to handle %s in eventReceiver: %s", e.EventName, err.Error())
			return err
		}
	case event.BucketCreated:
		err := globalDB.Load().Table(e.BucketName).AutoMigrate(&dbObject{})
		if err != nil {
			log.Error("failed to handle %s in eventReceiver: %s", e.EventName, err.Error())
			return err
		}
	case event.BucketRemoved:
		err := globalDB.Load().Table(e.BucketName).Migrator().DropTable(&dbObject{})
		if err != nil {
			log.Error("failed to handle %s in eventReceiver: %s", e.EventName, err.Error())
			return err
		}
	default:
		log.Info("ignored event(%s): %#v", e.EventName, e)
	}
	return nil
}

type dbObjectLayer struct {
	ObjectLayer
}

func verifyListObjsArgs(ctx context.Context, bucket, prefix string) error {
	if !IsValidObjectPrefix(prefix) {
		log.LogIf(ctx, ObjectNameInvalid{
			Bucket: bucket,
			Object: prefix,
		})
		return ObjectNameInvalid{
			Bucket: bucket,
			Object: prefix,
		}
	}
	return nil
}

func (l *dbObjectLayer) listObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	opts := listPathOptions{
		Bucket:      bucket,
		Prefix:      prefix,
		Separator:   delimiter,
		Limit:       maxKeysPlusOne(maxKeys, marker != ""),
		Marker:      marker,
		InclDeleted: false,
	}

	objects, err := l.listPath(ctx, &opts)
	if err != nil && err != io.EOF {
		if !isErrBucketNotFound(err) {
			log.LogIf(ctx, err)
		}
		return loi, err
	}

	loi.IsTruncated = err == nil && len(objects) > 0
	if maxKeys > 0 && len(objects) > maxKeys {
		objects = objects[:maxKeys]
		loi.IsTruncated = true
	}
	for _, obj := range objects {
		if obj.IsDir && obj.ModTime.IsZero() && delimiter != "" {
			loi.Prefixes = append(loi.Prefixes, obj.Name)
		} else {
			loi.Objects = append(loi.Objects, obj)
		}
	}
	if loi.IsTruncated {
		last := objects[len(objects)-1]
		loi.NextMarker = fmt.Sprintf("%s[minio_cache:%s,return:,p:%d]", last.Name, markerTagVersion, opts.pool+len(objects))
	}
	return loi, nil
}

func (l *dbObjectLayer) listPath(ctx context.Context, o *listPathOptions) (entries []ObjectInfo, err error) {
	if err := verifyListObjsArgs(ctx, o.Bucket, o.Prefix); err != nil {
		return entries, err
	}

	if o.Marker != "" && o.Prefix != "" {
		if !HasPrefix(o.Marker, o.Prefix) {
			return entries, io.EOF
		}
	}
	if o.Limit == 0 {
		return entries, io.EOF
	}

	if strings.HasPrefix(o.Prefix, SlashSeparator) {
		return entries, io.EOF
	}

	o.IncludeDirectories = o.Separator == slashSeparator
	if (o.Separator == slashSeparator || o.Separator == "") && !o.Recursive {
		o.Recursive = o.Separator != slashSeparator
		o.Separator = slashSeparator
	} else {
		o.Recursive = true
	}

	o.parseMarker()
	o.BaseDir = baseDirFromPrefix(o.Prefix)
	o.Transient = o.Transient || isReservedOrInvalidBucket(o.Bucket, false)
	o.SetFilter()
	if o.Transient {
		o.Create = false
	}

	q := globalDB.Load().Table(o.Bucket).Limit(o.Limit+1).Order("name").Group("name").
		Select("MAX(mod_time)", "*")
	if likeOnly || o.Separator != slashSeparator {
		if o.Prefix == "" || strings.HasSuffix(o.Prefix, "/") {
			q = q.Where("name LIKE ?", o.Prefix+"_%")
			if !o.Recursive {
				q = q.Where("name NOT LIKE ?", o.Prefix+"%"+o.Separator+"_%")
			}
		} else {
			q = q.Where("name = ?", o.Prefix)
		}
	} else {
		if !o.Recursive {
			if len(o.Prefix) > 0 {
				ns := strings.Split(strings.TrimSuffix(o.Prefix, slashSeparator), slashSeparator)
				q = q.Where(`json_array_length(path) = ?`, len(ns))
				for i, n := range ns {
					q = q.Where(`path ->> ? = ?`, i, n)
				}
			} else {
				q = q.Where("path is NULL")
			}
		} else {
			if len(o.Prefix) > 0 {
				ns := strings.Split(strings.TrimSuffix(o.Prefix, slashSeparator), slashSeparator)
				q = q.Where(`json_array_length(path) >= ?`, len(ns))
				for i, n := range ns {
					q = q.Where(`path ->> ? = ?`, i, n)
				}
			}
		}
	}
	if o.pool > 0 {
		q = q.Offset(o.pool)
	}
	tx := q.Find(&entries)
	if tx.Error != nil {
		return nil, tx.Error
	}
	truncated := len(entries) > o.Limit
	if truncated {
		return entries[:o.Limit], nil
	}
	return entries, io.EOF
}

func (l *dbObjectLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	s := time.Now()
	how := "db"
	defer func() {
		if e := recover(); e != nil {
			how = fmt.Sprintf("db->minio: %v", e)
			result, err = l.ObjectLayer.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
		}
		log.Info("ListObjects (%s) done, cost: %s", how, time.Now().Sub(s))
	}()
	co := listPathOptions{Marker: marker}
	co.parseMarker()
	if (co.ID == "" || co.Create) && globalDB.Load() != nil {
		result, err = l.listObjects(ctx, bucket, recoverLongObjectName(prefix), marker, delimiter, maxKeys)
		if err == nil {
			return
		}
	}
	how = "minio"
	return l.ObjectLayer.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
}

func (l *dbObjectLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	s := time.Now()
	how := "db"
	defer func() {
		if e := recover(); e != nil {
			how = fmt.Sprintf("db->minio: %v", e)
			result, err = l.ObjectLayer.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
		}
		log.Info("ListObjectsV2 (%s) done, cost: %s", how, time.Now().Sub(s))
	}()
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}
	co := listPathOptions{Marker: marker}
	co.parseMarker()
	if (co.ID == "" || co.Create) && globalDB.Load() != nil {
		loi, err := l.listObjects(ctx, bucket, recoverLongObjectName(prefix), marker, delimiter, maxKeys)
		if err == nil {
			result = ListObjectsV2Info{
				IsTruncated:           loi.IsTruncated,
				ContinuationToken:     continuationToken,
				NextContinuationToken: loi.NextMarker,
				Objects:               loi.Objects,
				Prefixes:              loi.Prefixes,
			}
			return result, nil
		}
	}
	how = "minio"
	return l.ObjectLayer.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
}

func (l *dbObjectLayer) listObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (loi ListObjectVersionsInfo, err error) {
	if marker == "" && versionMarker != "" {
		return loi, NotImplemented{}
	}

	o := listPathOptions{
		Bucket:      bucket,
		Prefix:      prefix,
		Separator:   delimiter,
		Limit:       maxKeysPlusOne(maxKeys, marker != ""),
		Marker:      marker,
		InclDeleted: true,
		Versioned:   true,
	}
	if err = verifyListObjsArgs(ctx, o.Bucket, o.Prefix); err != nil {
		return
	}
	if o.Marker != "" && o.Prefix != "" {
		if !HasPrefix(o.Marker, o.Prefix) {
			return
		}
	}
	if o.Limit == 0 {
		return
	}
	if strings.HasPrefix(o.Prefix, SlashSeparator) {
		return
	}

	o.IncludeDirectories = o.Separator == slashSeparator
	if (o.Separator == slashSeparator || o.Separator == "") && !o.Recursive {
		o.Recursive = o.Separator != slashSeparator
		o.Separator = slashSeparator
	} else {
		o.Recursive = true
	}

	o.parseMarker()
	o.BaseDir = baseDirFromPrefix(o.Prefix)
	o.Transient = o.Transient || isReservedOrInvalidBucket(o.Bucket, false)
	o.SetFilter()
	if o.Transient {
		o.Create = false
	}
	q := globalDB.Load().Table(o.Bucket).Limit(o.Limit + 1).Order("mod_time DESC")
	if o.Prefix == "" || strings.HasSuffix(o.Prefix, "/") {
		q = q.Where("name LIKE ?", o.Prefix+"_%")
		if !o.Recursive {
			q = q.Where("name NOT LIKE ?", o.Prefix+"%"+o.Separator+"_%")
		}
	} else if len(o.Prefix) > 0 {
		q = q.Where("name = ?", o.Prefix)
	}
	if o.pool > 0 {
		q = q.Offset(o.pool)
	}
	var objects []ObjectInfo
	tx := q.Find(&objects)
	if tx.Error != nil {
		return loi, tx.Error
	}
	if maxKeys > 0 && len(objects) > maxKeys {
		objects = objects[:maxKeys]
		loi.IsTruncated = true
	}
	for _, obj := range objects {
		if obj.IsDir && obj.ModTime.IsZero() && delimiter != "" {
			loi.Prefixes = append(loi.Prefixes, obj.Name)
		} else {
			loi.Objects = append(loi.Objects, obj)
		}
	}
	if loi.IsTruncated {
		last := objects[len(objects)-1]
		loi.NextMarker = fmt.Sprintf("%s[minio_cache:%s,return:,p:%d]", last.Name, markerTagVersion, o.pool+len(objects))
		loi.NextVersionIDMarker = last.VersionID
	}
	return loi, nil
}

func (l *dbObjectLayer) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (result ListObjectVersionsInfo, err error) {
	s := time.Now()
	how := "db"
	defer func() {
		if e := recover(); e != nil {
			how = fmt.Sprintf("db->minio: %v", e)
			result, err = l.ObjectLayer.ListObjectVersions(ctx, bucket, prefix, marker, versionMarker, delimiter, maxKeys)
		}
		log.Info("ListObjectVersions (%s) done, cost: %s", how, time.Now().Sub(s))
	}()
	co := listPathOptions{Marker: marker}
	co.parseMarker()
	log.Info("marker %#v", marker)
	if (co.ID == "" || co.Create) && globalDB.Load() != nil {
		result, err = l.listObjectVersions(ctx, bucket, recoverLongObjectName(prefix), marker, versionMarker, delimiter, maxKeys)
		if err == nil {
			return
		}
	}
	how = "minio"
	return l.ObjectLayer.ListObjectVersions(ctx, bucket, prefix, marker, versionMarker, delimiter, maxKeys)
}

func (l *dbObjectLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	s := time.Now()
	how := "db"
	defer func() {
		if e := recover(); e != nil {
			how = fmt.Sprintf("db->minio: %v", e)
			objInfo, err = l.ObjectLayer.GetObjectInfo(ctx, bucket, object, opts)
		}
		log.Info("GetObjectInfo (%s) done, cost: %s", how, time.Now().Sub(s))
	}()
	log.Info("GetObjectInfo opts:%#v", opts)
	db := globalDB.Load()
	if db != nil && bucket != ".minio.sys" {
		q := db.Table(bucket).Where("name = ?", recoverLongObjectName(object))
		if len(opts.VersionID) > 0 {
			q = q.Where("version_id = ?", opts.VersionID)
		}
		tx := q.First(&objInfo)
		if tx.Error == nil {
			objInfo.Bucket = bucket
			return
		}
	}
	how = "minio"
	return l.ObjectLayer.GetObjectInfo(ctx, bucket, object, opts)
}

func cast2ErasureServerPools(obj any) (result *erasureServerPools, ok bool) {
	switch o := obj.(type) {
	case *erasureServerPools:
		result = o
		ok = true
		return
	case *dbObjectLayer:
		return cast2ErasureServerPools(o.ObjectLayer)
	}
	return
}

func mustCast2ErasureServerPools(obj any) (result *erasureServerPools) {
	var ok bool
	result, ok = cast2ErasureServerPools(obj)
	if !ok {
		panic(fmt.Errorf("failed to cast %#v to *erasureServerPools", obj))
	}
	return
}
