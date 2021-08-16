package main

import (
	"context"
	"fmt"
	"gRPCDemo/book"
	"math/rand"
	"net"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/ratelimit"
	"github.com/go-kit/kit/sd/etcdv3"
	"github.com/go-kit/kit/tracing/zipkin"
	grpctransport "github.com/go-kit/kit/transport/grpc"
	opzipkin "github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter/http"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

type BookServer struct {
	bookListHandler grpctransport.Handler
	bookInfoHandler grpctransport.Handler
}

//通过grpc调用GetBookInfo时,GetBookInfo只做数据透传, 调用BookServer中对应Handler.ServeGRPC转交给go-kit处理
func (s *BookServer) GetBookInfo(ctx context.Context, in *book.BookInfoParams) (*book.BookInfo, error) {
	_, rsp, err := s.bookInfoHandler.ServeGRPC(ctx, in)
	if err != nil {
		return nil, err
	}
	return rsp.(*book.BookInfo), err
}

//通过grpc调用GetBookList时,GetBookList只做数据透传, 调用BookServer中对应Handler.ServeGRPC转交给go-kit处理
func (s *BookServer) GetBookList(ctx context.Context, in *book.BookListParams) (*book.BookList, error) {
	_, rsp, err := s.bookListHandler.ServeGRPC(ctx, in)
	if err != nil {
		return nil, err
	}
	return rsp.(*book.BookList), err
}

//创建bookList的EndPoint
func makeGetBookListEndpoint() endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		rand.Seed(time.Now().Unix())
		randInt := rand.Int63n(200)
		time.Sleep(time.Duration(randInt) * time.Millisecond)
		//请求列表时返回 书籍列表
		bl := new(book.BookList)
		bl.BookList = append(bl.BookList, &book.BookInfo{BookId: 1, BookName: "Go入门到精通"})
		bl.BookList = append(bl.BookList, &book.BookInfo{BookId: 2, BookName: "微服务入门到精通"})
		bl.BookList = append(bl.BookList, &book.BookInfo{BookId: 2, BookName: "区块链入门到精通"})
		return bl, nil
	}
}

//创建bookInfo的EndPoint
func makeGetBookInfoEndpoint() endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		rand.Seed(time.Now().Unix())
		randInt := rand.Int63n(200)
		time.Sleep(time.Duration(randInt) * time.Microsecond)
		//请求详情时返回 书籍信息
		req := request.(*book.BookInfoParams)
		b := new(book.BookInfo)
		b.BookId = req.BookId
		b.BookName = "Go入门系列"
		return b, nil
	}
}

func decodeRequest(_ context.Context, req interface{}) (interface{}, error) {
	return req, nil
}

func encodeResponse(_ context.Context, rsp interface{}) (interface{}, error) {
	return rsp, nil
}

func main() {

	var (
		//etcd服务地址
		etcdServer = "127.0.0.1:2379"
		//服务的信息目录
		prefix = "/services/book/"
		//当前启动服务实例的地址
		instance = "127.0.0.1:50061"
		//服务实例注册的路径
		key = prefix + instance
		//服务实例注册的val
		value = instance
		ctx   = context.Background()
		//服务监听地址
		serviceAddress = ":50061"
	)

	//etcd的连接参数
	options := etcdv3.ClientOptions{
		DialTimeout:   time.Second * 3,
		DialKeepAlive: time.Second * 3,
	}
	//创建etcd连接
	client, err := etcdv3.NewClient(ctx, []string{etcdServer}, options)
	if err != nil {
		panic(err)
	}

	// 创建注册器
	registrar := etcdv3.NewRegistrar(client, etcdv3.Service{
		Key:   key,
		Value: value,
	}, log.NewNopLogger())

	// 注册器启动注册
	registrar.Register()
	//启动追踪
	reporter := http.NewReporter("http://localhost:9411/api/v2/spans") //追踪地址
	defer reporter.Close()
	zkTracer, err := opzipkin.NewTracer(reporter)     //实例化追踪器
	zkServerTrace := zipkin.GRPCServerTrace(zkTracer) //追踪器Server端
	bookServer := new(BookServer)
	bookListEndPoint := makeGetBookListEndpoint()
	//创建限流器 1r/s  limiter := rate.NewLimiter(rate.Every(time.Second * 1), 100000)
	//通过DelayingLimiter中间件，在bookListEndPoint的外层再包裹一层限流的endPoint
	limiter := rate.NewLimiter(rate.Every(time.Second*3), 1) //限流每隔r秒生成1个，临牌容量
	// limiter := rate.NewLimiter(3, 1)                         //限流每秒生成r个令牌，临牌容量
	bookListEndPoint = ratelimit.NewDelayingLimiter(limiter)(bookListEndPoint)

	bookListHandler := grpctransport.NewServer(
		bookListEndPoint,
		decodeRequest,
		encodeResponse,
		zkServerTrace, //添加追踪
	)
	bookServer.bookListHandler = bookListHandler

	bookInfoEndPoint := makeGetBookInfoEndpoint()
	//通过DelayingLimiter中间件，在bookListEndPoint的外层再包裹一层限流的endPoint
	bookInfoEndPoint = ratelimit.NewDelayingLimiter(limiter)(bookInfoEndPoint)
	bookInfoHandler := grpctransport.NewServer(
		bookInfoEndPoint,
		decodeRequest,
		encodeResponse,
		zkServerTrace,
	)
	bookServer.bookInfoHandler = bookInfoHandler

	ls, err := net.Listen("tcp", serviceAddress)
	if err != nil {
		fmt.Println(err)
		return
	}
	gs := grpc.NewServer(grpc.UnaryInterceptor(grpctransport.Interceptor))
	book.RegisterBookServiceServer(gs, bookServer)
	gs.Serve(ls)
}
