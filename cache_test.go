package pghub

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"
)

//import PgHub from "../src/index.mjs";
//import assert from 'assert';

var stmt0 = "create table test (\n" +
	" id bigint PRIMARY KEY,\n" +
	" col1 varchar(40) NOT NULL,\n" +
	" col2 integer NOT NULL\n" +
	");"

var stmt1 = "create or replace function fun_notify_tab_stm() returns trigger\n" +
	"as $$\n" +
	" declare key_count integer := 0;\n" +
	" declare key_list text := '';\n" +
	" declare payload text;\n" +
	" declare channel text := TG_TABLE_NAME || '_stm';\n" +
	" declare key_max integer = 300;\n" +
	" begin" +
	"  if (TG_OP = 'DELETE') then\n" +
	"   select count(id) from old_table into key_count;\n" +
	"   if (key_count < key_max) then\n" +
	"    select string_agg(id::text, ',') from old_table into key_list;\n" +
	"   end if;\n" +
	"  end if;\n" +
	"  if (TG_OP = 'UPDATE') then\n" +
	"   select distinct count(id) from ((select id from new_table) union (select id from old_table)) into key_count;\n" +
	"   if (key_count < key_max) then\n" +
	"    select distinct string_agg(id::text, ',') from ((select id from new_table) union (select id from old_table)) into key_list;\n" +
	"   end if;\n" +
	"  end if;\n" +
	"  if (TG_OP = 'INSERT') then\n" +
	"   select count(id) from new_table into key_count;\n" +
	"   if (key_count < key_max) then\n" +
	"    select string_agg(id::text, ',') from new_table into key_list;\n" +
	"   end if;\n" +
	"  end if;\n" +
	"  payload :=" +
	"   'e' ||\n" +
	"   key_list ||\n" +
	"   ':{\"op\":\"' ||\n" +
	"   lower(substring(TG_OP,1,3)) ||\n" +
	"   '\",\"count\":' ||\n" +
	"   key_count ||\n" +
	"   '}';\n" +
	"  perform pg_notify(channel, payload);\n" +
	"  return NULL;\n" +
	"  end;\n" +
	"$$ language plpgsql;"

var stmt2 = "create trigger test_notify_stm_ins after insert on test\n" +
	"referencing new table as new_table\n" +
	"for each statement execute function fun_notify_tab_stm();"

var stmt3 = "create trigger test_notify_stm_upd after update on test\n" +
	"referencing old table as old_table new table as new_table\n" +
	"for each statement execute function fun_notify_tab_stm();"

var stmt4 = "create trigger test_notify_stm_del after delete on test\n" +
	"referencing old table as old_table\n" +
	"for each statement execute function fun_notify_tab_stm();"

var stmt5 = "drop table if exists test;"

var stmt6 = "drop function if exists fun_notify_tab_stm;"

var setup = []string{
	stmt5, stmt6, stmt0, stmt1, stmt2, stmt3, stmt4}

var cleanup = []string{stmt5, stmt6}

func runSequence(pool *pgxpool.Pool, statements []string) error {
	for _, statement := range statements {
		_, err := pool.Exec(context.Background(), statement)

		if err != nil {
			return err
		}
	}

	return nil
}

type CacheRow struct {
	col1 string
}

type Cache struct {
	pool     *pgxpool.Pool
	hub      *Hub
	consumer *Consumer
	rows     map[int64]*CacheRow
	ch       chan *Event
	magic    int64
	complete chan error
	begin    sync.Mutex
}

func NewCache(pool *pgxpool.Pool, hub *Hub, magic int64) (*Cache, error) {
	cache := Cache{}
	var err error

	cache.ch = make(chan *Event, 1000)
	cache.complete = make(chan error)
	cache.magic = magic
	cache.pool = pool
	cache.hub = hub
	cache.rows = make(map[int64]*CacheRow)
	cache.consumer, err = hub.Consumer(func(event *Event) { cache.ch <- event }, "test_stm")

	//	cache.complete.Lock()
	cache.begin.Lock()

	go cache.worker()

	return &cache, err
}

func (c *Cache) waitComplete() error {
	return <-c.complete
}

func (c *Cache) waitBegin() {
	c.begin.Lock()
	c.begin.Unlock()
}

//func (cache *Cache) close() {
//	cache.consumer.Close()
//	cache.consumer = nil
//}

func (cache *Cache) confirm(length int) bool {
	if len(cache.rows) == length {
		return false
	}

	quartile1 := length / 4
	quartile2 := (length / 2)
	quartile3 := (3 * (length / 4))

	var i int

	for i = 1; i != length; i++ {
		row := cache.rows[int64(i)]

		if i < quartile1 {
			if row.col1 != "quartile 1" {
				return false
			}
		} else if i < quartile2 {
			if row.col1 != "quartile 2" {
				return false
			}
		} else if i < quartile3 {
			if row.col1 != "quartile 3" {
				return false
			}
		} else if row.col1 != "quartile 4" {
			return false
		}
	}

	return true
}

func (cache *Cache) worker() {
	var err error
	done := false

	for !done {
		e := <-cache.ch
		done, err = cache.handleEvent(e)
	}

	cache.complete <- err
}

func (cache *Cache) handleEvent(event *Event) (bool, error) {
	pool := cache.pool
	if event.Topic() == "" {
		advisory := event.Value().(*Advisory)

		if advisory.action == ActionClose {
			return true, nil
		} else if advisory.action == ActionSubscribe {
			cache.begin.Unlock()
		}
	} else {
		value := event.Value().(map[string]interface{})
		keys := event.Key()

		switch value["op"].(string) {
		case "ins":
			{
				for _, key := range keys {
					rows, err1 := pool.Query(context.Background(), "select * from test where id=$1", key)

					if err1 != nil {
						return true, err1
					}

					cacheRow := &CacheRow{}

					for rows.Next() {
						values, err2 := rows.Values()

						if err2 != nil {
							rows.Close()
							return true, err2
						}

						cacheRow.col1 = values[1].(string)
						cache.rows[key] = cacheRow
					}

					rows.Close()
				}

				break
			}

		case "upd":
			{
				for _, key := range keys {
					rows, err1 := pool.Query(context.Background(), "select * from test where id=$1", key)

					if err1 != nil {
						return true, err1
					}

					cacheRow := &CacheRow{}

					for rows.Next() {
						values, err2 := rows.Values()

						if err2 != nil {
							rows.Close()
							return true, err2
						}

						cacheRow.col1 = values[1].(string)
						cache.rows[key] = cacheRow

						if key == cache.magic {
							cache.consumer.Close()
						}
					}

					rows.Close()
				}

				break
			}

		case "del":
			{
				for _, key := range keys {
					cache.rows[key] = nil
				}

				break
			}
		}
	}

	return false, nil
}

func execDualCache(pool *pgxpool.Pool, hub *Hub) error {
	const rowCount = 256
	cache0, err0 := NewCache(pool, hub, rowCount-1)

	if err0 != nil {
		return err0
	}

	cache1, err1 := NewCache(pool, hub, rowCount-1)

	if err1 != nil {
		return err1
	}

	cache0.waitBegin()
	cache1.waitBegin()

	for i := 0; i != rowCount; i++ {
		_, err2 := pool.Exec(
			context.Background(),
			"insert into test (id, col1, col2) values ($1,$2,$3)",
			int64(i), "quartile 4", i%16)

		if err2 != nil {
			return err2
		}
	}

	_, err3 := pool.Exec(
		context.Background(),
		"update test set col1=$1 where id<$2",
		"quartile 3", (rowCount*3)/4)

	if err3 != nil {
		return err3
	}

	_, err4 := pool.Exec(
		context.Background(),
		"update test set col1=$1 where id<$2",
		"quartile 2", (rowCount*2)/4)

	if err4 != nil {
		return err4
	}

	_, err5 := pool.Exec(
		context.Background(),
		"update test set col1=$1 where id<$2",
		"quartile 1", (rowCount*1)/4)

	if err3 != nil {
		return err3
	}

	for i := rowCount; i < rowCount+100; i++ {
		_, err6 := pool.Exec(
			context.Background(),
			"insert into test (id, col1, col2) values ($1,$2,$3)",
			int64(i), "quartile 4", i%16)

		if err6 != nil {
			return err5
		}
	}

	_, err7 := pool.Exec(
		context.Background(),
		"delete from test where id>$1",
		rowCount-1)

	if err7 != nil {
		return err7
	}

	_, err8 := pool.Exec(
		context.Background(),
		"update test set col2=$1 where id=$2",
		-1, rowCount-1)

	if err8 != nil {
		return err8
	}

	err10 := cache0.waitComplete()
	err11 := cache1.waitComplete()

	//	cache0.complete.Unlock()
	//	cache1.complete.Unlock()

	if err10 == nil {
		return err10
	}
	if err11 == nil {
		return err11
	}

	success1 := cache0.confirm(rowCount)
	success2 := cache1.confirm(rowCount)

	if success1 && success2 {
		return nil
	} else {
		return errors.New("cache not confirmed")
	}
}

func Test2(t *testing.T) {
	pool, err1 := initPgxPool(newTestConfig())

	if err1 != nil {
		t.Fatal(err1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	SetLogger(logger)

	err0 := runSequence(pool, setup)

	if err0 != nil {
		t.Fatal(err0)
	}

	config := &Config{}

	config.Logger = logger
	config.Pool = pool
	config.ConnectRetry = time.Second * 5

	hub, err2 := New("test", config)

	if err2 != nil {
		t.Fatal(err2)
	}

	err3 := hub.Start()

	if err3 != nil {
		t.Fatal(err3)
	}

	err4 := execDualCache(pool, hub)
	if err4 != nil {
		t.Fatal(err4)
	}

	err5 := runSequence(pool, cleanup)
	if err5 != nil {
		t.Fatal(err3)
	}

	err6 := hub.Stop(true)
	if err6 != nil {
		t.Fatal(err3)
	}

	var stats1 Stats
	var stats2 HubStats
	GetStats(&stats1)
	hub.GetStats(&stats2)

	if stats1.Valid || stats2.Valid {
		logger.Info("stats")
	}
}
