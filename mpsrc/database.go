package mpsrc

import (
	// "fmt"
	"database/sql"
	"golang.org/x/net/context"
)

func getInfo(tablename, vt, userID, key string) []uint64 {

	var userIDs []uint64
	var uid uint64
	client := mysqlPool.GetClient(false)
	if client == nil {
		mpLogger.Error(ErrAllMysqlDown)
		return userIDs
	}
	rows, err := client.Query("select "+vt+" from "+tablename+" where uid=? order by ts ASC", userID)
	switch err {
		case sql.ErrNoRows:
			//回写脏数据
			storageProxy.Set(context.Background(), key, ErrorResult)
			return userIDs
		case nil:
		default :
			mpLogger.Warn(err)
			return userIDs 
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&uid)
		if err != nil {
			mpLogger.Warn(err)
			continue
		}
		userIDs = append(userIDs, uid)
	}
	//set cache
	go func(userIDs []uint64, key string) {
		if item, _, err := setItem(userIDs, 0); err == nil {
			storageProxy.Set(context.Background(), key, item)
		}
	}(userIDs, key)
	return userIDs
}

func getFriendsInfoFromDB(userID string, infoType, key string) []uint64 {
	switch infoType {
	case FANS:
		return getInfo("fanslist", "fid", userID, key)
	case LIKES:
		return getInfo("likeslist", "lid", userID, key)
	default:
		//todo
	}
	return nil
}
 
func updateFriendsInfoOfDB(vt, tablename, opt string, userID, value uint64) {
	client := mysqlPool.GetClient(true)
	if client == nil {
		mpLogger.Error(ErrAllMysqlDown)
		return
	}
	switch opt {
	case "add":
		if _, err := client.Exec("insert into "+tablename+"(uid, "+vt+" ,ts) values(?,?,now())", userID, value); err != nil {
			mpLogger.Warn(err)
			return
		}
	case "delete":
		if _, err := client.Exec("delete from "+tablename+" where uid=? and "+vt+"=?", userID, value); err != nil {
			mpLogger.Warn(err)
			return
		}
		//附加操作（删除pushtimeline里对方的内容）
		delPushFriendsTimeline(userID, value)
	default:
		//do nothing
	}
}

func getPersonalTimelineKeyFromDB(uid, tb, te uint64, key string) Timelines {

	var (
		valuekey string
		ts uint64
		rows *sql.Rows
		err error
	)
	timelinekeys := make(Timelines, 0) 
	client := mysqlPool.GetClient(false)
	if client == nil {
		mpLogger.Error(ErrAllMysqlDown)
		return timelinekeys
	}
	switch hash(uid) {
	case 0:
		rows, err = client.Query("select valuekey, ts from personaltimeline1 where uid=? and ts > ? and ts < ?", uid, tb, te)
	case 1:
		rows, err = client.Query("select valuekey, ts from personaltimeline1 where uid=? and ts > ? and ts < ?", uid, tb, te)
	}
	switch err {
		case sql.ErrNoRows:
			//回写脏数据
			storageProxy.Set(context.Background(), key, ErrorResult)
			return timelinekeys
		case nil:
		default :
			mpLogger.Warn(err)
			return timelinekeys 
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&valuekey, &ts)
		if err != nil {
			mpLogger.Warn(err)
			continue
		}
		timelinekey := new(TimelineKey)
		timelinekey.Timestamp = ts 
		timelinekey.UserID = uid 
		timelinekey.ValueKey = valuekey
		timelinekeys = append(timelinekeys, timelinekey)
	}
	//set cache
	go func(timelinekeys Timelines, key string) {
		if item, _, err := setItem(timelinekeys, 0); err == nil {
			storageProxy.Set(context.Background(), key, item)
		}
	}(timelinekeys, key)
	return timelinekeys
}

func addPersonalTimelineOfDB(uid, ts uint64, valuekey string) {
	client := mysqlPool.GetClient(true)
	if client == nil {
		mpLogger.Error(ErrAllMysqlDown)
		return 
	}
	switch hash(uid) {
	case 0:
		if _, err := client.Exec("insert into personaltimeline1(uid, ts, valuekey) values(?,?,?)", uid, ts, valuekey); err != nil {
			mpLogger.Warn(err)
			return
		}
	case 1:
		if _, err := client.Exec("insert into personaltimeline2(uid, ts, valuekey) values(?,?,?)", uid, ts, valuekey); err != nil {
			mpLogger.Warn(err)
			return
		}
	}
	//set cache
}

func deletePersonalTimelineOfDB(uid, ts uint64) {
	client := mysqlPool.GetClient(true)
	if client == nil {
		mpLogger.Error(ErrAllMysqlDown)
		return 
	}
	switch hash(uid) {
	case 0:
		if _, err := client.Exec("delete from personaltimeline1 where uid=? and ts=?", uid, ts); err != nil {
			mpLogger.Warn(err)
			return
		}
	case 1:
		if _, err := client.Exec("delete from personaltimeline2 where uid=? and ts=?", uid, ts); err != nil {
			mpLogger.Warn(err)
			return
		}
	}
	//set cache
}

func updatePersonalTimeline(uid, ts uint64, valuekey, opt string)  {
	switch opt {
	case "add":
		addPersonalTimelineOfDB(uid, ts, valuekey)
	case "delete":
		deletePersonalTimelineOfDB(uid, ts)
	default:
		//do nothing
	}
}

func getPushFriendsTimelineFromDB(userID, tb, te uint64, key string) (Timelines, error) {

	var (
		likesid uint64
		ts uint64
		valuekey string
	)
	timelinekeys := make(Timelines, 0) 
	client := mysqlPool.GetClient(false)
	if client == nil {
		mpLogger.Error(ErrAllMysqlDown)
		return timelinekeys, ErrAllMysqlDown
	}
	rows, err := client.Query("select lid, ts, valuekey from pushfriendstimeline where uid=? and ts>? and ts<?", userID, tb, te)
	switch err {
		case sql.ErrNoRows:
			//回写脏数据
			storageProxy.Set(context.Background(), key, ErrorResult)
			return timelinekeys, nil
		case nil:
		default :
			mpLogger.Warn(err)
			return timelinekeys, err 
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&likesid, &ts, &valuekey)
		if err != nil {
			return timelinekeys, err
		}
		timelinekey := new(TimelineKey)
		timelinekey.Timestamp = ts 
		timelinekey.UserID = likesid 
		timelinekey.ValueKey = valuekey
		timelinekeys = append(timelinekeys, timelinekey)
	}
	//set cache
	go func(timelinekeys Timelines, key string) {
		if item, _, err := setItem(timelinekeys, 0); err == nil {
			storageProxy.Set(context.Background(), key, item)
		}
	}(timelinekeys, key)
	return timelinekeys, nil
}

func addPushFriendsTimeline(userID, likesID, ts uint64, valuekey string) {
	client := mysqlPool.GetClient(true)
	if client == nil {
		mpLogger.Error(ErrAllMysqlDown)
		return
	}
	if _, err := client.Exec("insert into pushfriendstimeline(uid, lid, ts, valuekey) values(?,?,?,?)", userID, likesID, ts, valuekey); err != nil {
		mpLogger.Warn(err)
		return
	}
	//set cache
}

func delPushFriendsTimeline(userID, likesID uint64) {
	client := mysqlPool.GetClient(true)
	if client == nil {
		mpLogger.Error(ErrAllMysqlDown)
		return
	}
	if _, err := client.Exec("delete from pushfriendstimeline where uid=?, lid=?", userID, likesID); err != nil {
		mpLogger.Warn(err)
		return
	}
	//set cache
}

func addValueToDB(valueKey, value string) {
	client := mysqlPool.GetClient(true)
	if client == nil {
		mpLogger.Error(ErrAllMysqlDown)
		return 
	}
	_, err := client.Exec("insert into valuestore(valuekey, value) values(?,?)", valueKey, value)
	if err != nil {
		mpLogger.Warn(err)
		return 
	}
	//set cache

}

func delValueFromDB(value string) {
	client := mysqlPool.GetClient(true)
	if client == nil {
		mpLogger.Error(ErrAllMysqlDown)
	}
	_, err := client.Exec("delete from valuestore where value=?", value)
	if err != nil {
		mpLogger.Warn(err)
	}
	//set cache
}

func getValueFromDB(valueKey string) string {
	var value string
	client := mysqlPool.GetClient(false)
	if client == nil {
		mpLogger.Error(ErrAllMysqlDown)
		return value
	}
	rows, err := client.Query("select value from valuestore where valuekey=?", valueKey)
	switch err {
		case sql.ErrNoRows:
			//回写脏数据
			storageProxy.Set(context.Background(), valueKey, ErrorResult)
			return value
		case nil:
		default :
			mpLogger.Warn(err)
			return value 
	}
	defer rows.Close()
	if rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			mpLogger.Warn(err)
		}
	}
	//set cache
	go func(value, key string) {
		if item, _, err := setItem(value, 0); err == nil {
			storageProxy.Set(context.Background(), key, item)
		}
	}(value, valueKey)
	return value
}
