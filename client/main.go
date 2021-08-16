package main

import (
	"context"
	"fmt"
	"gRPCDemo/book"
	"io"
	"time"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/go-kit/kit/circuitbreaker"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/sd"
	"github.com/go-kit/kit/sd/etcdv3"
	"github.com/go-kit/kit/sd/lb"
	"github.com/go-kit/kit/tracing/zipkin"
	grpctransport "github.com/go-kit/kit/transport/grpc"
	opzipkin "github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter/http"
	"google.golang.org/grpc"
)

func main() {
	var (
		//注册中心地址
		etcdServer = "127.0.0.1:2379"
		//监听的服务前缀
		prefix = "/services/book/"
		ctx    = context.Background()
	)
	//对hystrix进行配置
	commandName := "my_endpoint"
	hystrix.ConfigureCommand(commandName, hystrix.CommandConfig{
		Timeout:                1000 * 30, //超时
		MaxConcurrentRequests:  100,       //最大并发的请求数
		RequestVolumeThreshold: 5,         //请求量阈值
		SleepWindow:            1000 * 10, //熔断开启多久尝试发起一次请求
		ErrorPercentThreshold:  1,         //误差阈值百分比
	})
	breakerMw := circuitbreaker.Hystrix(commandName) //定义熔断器中间件
	options := etcdv3.ClientOptions{
		DialTimeout:   time.Second * 3,
		DialKeepAlive: time.Second * 3,
	}
	//连接注册中心
	client, err := etcdv3.NewClient(ctx, []string{etcdServer}, options)
	if err != nil {
		panic(err)
	}
	logger := log.NewNopLogger()
	//创建实例管理器, 此管理器会Watch监听etc中prefix的目录变化更新缓存的服务实例数据
	instancer, err := etcdv3.NewInstancer(client, prefix, logger)
	if err != nil {
		panic(err)
	}
	//创建端点管理器， 此管理器根据Factory和监听的到实例创建endPoint并订阅instancer的变化动态更新Factory创建的endPoint
	endpointer := sd.NewEndpointer(instancer, reqFactory, logger)
	//创建负载均衡器
	balancer := lb.NewRoundRobin(endpointer)

	/**
	我们可以通过负载均衡器直接获取请求的endPoint，发起请求
	reqEndPoint,_ := balancer.Endpoint()
	*/
	/**
	也可以通过retry定义尝试次数进行请求
	*/
	reqEndPoint := lb.Retry(3, 100*time.Second, balancer) //请求次数为3，时间为10S（时间需要多于服务器限流时间3s）

	//增加熔断中间件
	reqEndPoint = breakerMw(reqEndPoint)
	//现在我们可以通过 endPoint 发起请求了

	req := struct{}{}
	for i := 1; i <= 30; i++ {
		if _, err = reqEndPoint(ctx, req); err != nil {
			fmt.Println(err)
		}
	}
}

//通过传入的 实例地址  创建对应的请求endPoint
func reqFactory(instanceAddr string) (endpoint.Endpoint, io.Closer, error) {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		fmt.Println("请求服务: ", instanceAddr, "当前时间: ", time.Now().Format("2006-01-02 15:04:05.99"))
		conn, err := grpc.Dial(instanceAddr, grpc.WithInsecure())
		if err != nil {
			fmt.Println(err)
			panic("connect error")
		}

		//追踪设置
		reporter := http.NewReporter("http://localhost:9411/api/v2/spans") //追踪地址
		defer reporter.Close()

		zkTracer, err := opzipkin.NewTracer(reporter)     //新建追踪器
		zkClientTrace := zipkin.GRPCClientTrace(zkTracer) //启动追踪器Client端

		bookInfoRequest := grpctransport.NewClient(
			conn,
			"book.BookService",
			"GetBookInfo",
			func(_ context.Context, in interface{}) (interface{}, error) { return nil, nil },
			func(_ context.Context, out interface{}) (interface{}, error) {
				return out, nil
			},
			book.BookInfo{},
			zkClientTrace, //追踪客户端
		).Endpoint()

		bookListRequest := grpctransport.NewClient(
			conn,
			"book.BookService",
			"GetBookList",
			func(_ context.Context, in interface{}) (interface{}, error) { return nil, nil },
			func(_ context.Context, out interface{}) (interface{}, error) {
				return out, nil
			},
			book.BookList{},
			zkClientTrace,
		).Endpoint()

		parentSpan := zkTracer.StartSpan("bookCaller")
		defer parentSpan.Flush()

		ctx = opzipkin.NewContext(ctx, parentSpan)
		infoRet, err := bookInfoRequest(ctx, request)
		if err != nil {
			return nil, err
		}
		bi := infoRet.(*book.BookInfo)
		fmt.Println("获取书籍详情")
		fmt.Println("bookId: 1", " => ", "bookName:", bi.BookName)

		listRet, err := bookListRequest(ctx, request)
		if err != nil {
			return nil, err
		}
		bl := listRet.(*book.BookList)
		fmt.Println("获取书籍列表")
		for _, b := range bl.BookList {
			fmt.Println("bookId:", b.BookId, " => ", "bookName:", b.BookName)
		}

		return nil, nil
	}, nil, nil
}
