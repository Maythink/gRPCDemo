package main

import (
	"context"
	"fmt"
	"gRPCDemo/book"
	"net"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/ratelimit"
	"github.com/go-kit/kit/sd/etcdv3"
	grpc_transport "github.com/go-kit/kit/transport/grpc"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

type BookServer struct {
	bookListHandler grpc_transport.Handler
	bookInfoHandler grpc_transport.Handler
}

//一下两个方法实现了 protoc生成go文件对应的接口：
/*
// BookServiceServer is the server API for BookService service.
type BookServiceServer interface {
	GetBookInfo(context.Context, *BookInfoParams) (*BookInfo, error)
	GetBookList(context.Context, *BookListParams) (*BookList, error)
}
*/
//通过grpc调用GetBookInfo时,GetBookInfo只做数据透传, 调用BookServer中对应Handler.ServeGRPC转交给go-kit处理
func (s *BookServer) GetBookInfo(ctx context.Context, in *book.BookInfoParams) (*book.BookInfo, error) {

	_, rsp, err := s.bookInfoHandler.ServeGRPC(ctx, in)
	if err != nil {
		return nil, err

	}
	/*
		if info,ok:=rsp.(*book.BookInfo);ok {
			return info,nil
		}
		return nil,errors.New("rsp.(*book.BookInfo)断言出错")
	*/
	return rsp.(*book.BookInfo), err //直接返回断言的结果
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
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		b := new(book.BookList)
		b.BookList = append(b.BookList, &book.BookInfo{BookId: 1, BookName: "Go语言入门到精通"})
		b.BookList = append(b.BookList, &book.BookInfo{BookId: 2, BookName: "微服务入门到精通"})
		b.BookList = append(b.BookList, &book.BookInfo{BookId: 2, BookName: "区块链入门到精通"})
		return b, nil
	}
}

//创建bookInfo的EndPoint
func makeGetBookInfoEndpoint() endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		//请求详情时返回 书籍信息
		req := request.(*book.BookInfoParams)
		b := new(book.BookInfo)
		b.BookId = req.BookId
		b.BookName = "Go入门到精通"
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
		etcdServer     = "127.0.0.1:2379"        //etcd服务的IP地址
		prefix         = "/services/book/"       //服务的目录
		ServerInstance = "127.0.0.1:50052"       //当前实例Server的地址
		key            = prefix + ServerInstance //服务实例注册的路径
		value          = ServerInstance
		ctx            = context.Background()
		//服务监听地址
		serviceAddress = ":50052"
	)
	//etcd连接参数
	option := etcdv3.ClientOptions{DialTimeout: time.Second * 3, DialKeepAlive: time.Second * 3}
	//创建连接
	client, err := etcdv3.NewClient(ctx, []string{etcdServer}, option)
	if err != nil {
		panic(err)
	}
	//创建注册
	registrar := etcdv3.NewRegistrar(client, etcdv3.Service{Key: key, Value: value}, log.NewNopLogger())
	registrar.Register() //启动注册服务
	bookServer := new(BookServer)
	bookInfoEndPoint := makeGetBookInfoEndpoint()
	//rate路径：golang.org/x/time/rate
	limiter := rate.NewLimiter(rate.Every(time.Second*3), 1) //限流3秒，临牌数：1
	//通过DelayingLimiter中间件，在bookInfoEndPoint 的外层再包裹一层限流的endPoint
	bookInfoEndPoint = ratelimit.NewDelayingLimiter(limiter)(bookInfoEndPoint)
	bookListHandler := grpc_transport.NewServer(
		makeGetBookListEndpoint(),
		decodeRequest,
		encodeResponse,
	)
	bookServer.bookListHandler = bookListHandler
	bookInfoHandler := grpc_transport.NewServer(
		bookInfoEndPoint,
		decodeRequest,
		encodeResponse,
	)
	bookServer.bookInfoHandler = bookInfoHandler
	listener, err := net.Listen("tcp", serviceAddress) //网络监听，注意对应的包为："net"
	if err != nil {
		fmt.Println(err)
		return
	}
	gs := grpc.NewServer(grpc.UnaryInterceptor(grpc_transport.Interceptor))
	book.RegisterBookServiceServer(gs, bookServer) //调用protoc生成的代码对应的注册方法
	gs.Serve(listener)                             //启动Server

}
