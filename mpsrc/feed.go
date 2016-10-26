package mpsrc

import (
	"flag"
	"fmt"
	mtlog "gitlab.meitu.com/platform/gocommons/log"
	"feed/storage"
	"golang.org/x/net/context"
	"feed/lib"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
)

type Flags struct {
	ver  bool
	cmd  string
	conf string
}

var (
	argsflag     = Flags{}
	redisPool    *lib.RedisPool
	mcStorage    *storage.MemcacheStorage
	storageProxy storage.DefaultProxy
	mysqlPool    *lib.MysqlPool
	config       *TomlConfig
	mpLogger     mtlog.Logger
	adminServer  *AdminHttpServer
	httpServer   *HttpServer
	err          error
)

func parseFlags() {
	flag.BoolVar(&argsflag.ver, "v", false, "Show Version")
	flag.StringVar(&argsflag.conf, "c", DEFAULT_CONF, "conf file path")
	flag.Parse()

	if argsflag.ver {
		fmt.Printf("MeituDns %s\n", VERSION)
		os.Exit(0)
	}
}

/*
* 根据用户指定参数来设置 cpu 数目
* @param cpuNum: 用户指定的 cpu 数目
 */
func setCPUNum(cpuNum int) {
	if cpuNum == 0 || cpuNum > runtime.NumCPU() {
		cpuNum = runtime.NumCPU()
	}
	mpLogger.Debug("Run with cpus: ", cpuNum)
	runtime.GOMAXPROCS(cpuNum)
}

func setupHttpServer(config *HttpConfig, logDir string) {
	httpServer = &HttpServer{}
	go httpServer.StartHttp(config.Host, config.Port, config.NeedAccessLog, logDir)
}

func setupAdminServer(config *HttpConfig, logDir string) {
	adminServer = &AdminHttpServer{}
	go adminServer.StartHttp(config.Host, config.Port, config.NeedAccessLog, logDir)
}

func setMysqlPool() {
	var err error
	opts := &lib.MysqlOptions{}
	if config.DB.MaxConns > 0 {
		opts.MaxActive = config.DB.MaxConns
	}
	if config.DB.MaxIdle > 0 {
		opts.MaxIdle = config.DB.MaxIdle
	}
	if config.DB.ConnectTimeout > 0 {
		opts.ConnectTimeout = config.DB.ConnectTimeout
	}
	if config.DB.ReadTimeout > 0 {
		opts.ReadTimeout = config.DB.ReadTimeout
	}
	mysqlPool, err = lib.NewMysqlPool(config.DB.Master, config.DB.Slaves,
		config.DB.User, config.DB.Passwd, config.DB.DBName, opts)
	if err != nil {
		fmt.Println("Setup mysql pool failed", err)
		os.Exit(1)
	}
}

func setupStorageProxy() {
	storageProxy = storage.DefaultProxy{
		PreferredStorage: mcStorage,
	}
}

func setupRedisPool() {
	opts := &lib.RedisOption{}
	if config.Redis.MaxConns > 0 {
		opts.MaxActive = config.Redis.MaxConns
	}
	if config.Redis.ConnectTimeout > 0 {
		opts.ConnectTimeout = config.Redis.ConnectTimeout
	}
	if config.Redis.ReadTimeout > 0 {
		opts.ReadTimeout = config.Redis.ReadTimeout
	}
	if config.Redis.IdleTimeout > 0 {
		opts.IdleTimeout = config.Redis.IdleTimeout
	}
	if config.Redis.MaxIdle > 0 {
		opts.MaxIdle = config.Redis.MaxIdle
	}
	if config.Redis.Auth != "" {
		opts.Auth = config.Redis.Auth
	}
	opts.DB = config.Redis.DB
	opts.Retries = 2 // 重试两次
	redisPool = lib.NewRedisPool(config.Redis.Master, config.Redis.Slaves, opts)
}

func setupMemcacheStorage() {
	opts := &lib.MemcachedStorageOpts{}
	if config.Memcached.SetBackExpiration > 0 {
		opts.SetBackExpiration = config.Memcached.SetBackExpiration
	}
	if config.Memcached.ReplicaExpiration > 0 {
		opts.ReplicaExpiration = config.Memcached.ReplicaExpiration
	}
	if config.Memcached.ReplicaWritePolicy >= 0 {
		opts.ReplicaWritePolicy = config.Memcached.ReplicaWritePolicy
	}
	opts.SetBackMaster = config.Memcached.SetBackMaster
	opts.LE = func(c context.Context, err error, v ...interface{}) {
		mpLogger.Error(err, v)
	}
	opts.LI = func(c context.Context, v ...interface{}) {
		mpLogger.Info(v)
	}
	mcStorage, err = lib.NewMemcachedStorage(config.Memcached.Master, config.Memcached.Slave, config.Memcached.Replicas, opts)
	if err != nil {
		fmt.Println("Setup mc failed", err)
		os.Exit(1)
	}
}

func startCPUProfile() {

	f, err := os.Create("test.pprof")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can not create cpu profile output file: %s",
			err)
		return
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		fmt.Fprintf(os.Stderr, "Can not start cpu profile: %s", err)
		f.Close()
		return
	}

}

func stopCPUProfile() {
	pprof.StopCPUProfile() // 把记录的概要信息写到已指定的文件

}

func Main() {
	var err error
	// startCPUProfile()
	// defer stopCPUProfile()
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	parseFlags()
	config, err = loadConfig(argsflag.conf)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// NOTICE: 这个函数之前都不允许调用 log 函数
	mpLogger, err = mtlog.CreateSeelogger("feed")
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
	setCPUNum(config.CpuNum)
	setMysqlPool()
	setupRedisPool()
	setupMemcacheStorage()
	setupStorageProxy()
	setupHttpServer(&config.Http, config.LogDir)
	setupAdminServer(&config.Admin, config.LogDir)
	
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	for {
		select {
		case <-signals:
			//TODO: close()
			stopKafka()
			return
		}
	}

}
