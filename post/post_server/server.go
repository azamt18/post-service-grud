package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"os"
	"os/signal"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"post/post/postpb"
)

var collection *mongo.Collection

type server struct{}

func (s server) GetPosts(request *postpb.GetPostsRequest, stream postpb.PostService_GetPostsServer) error {
	fmt.Println("Get posts request...")

	cursor, error := collection.Find(context.Background(), primitive.D{{}}) // D - used because of the order of the elements matters
	if error != nil {
		return status.Errorf(
			codes.Internal,
			fmt.Sprintf("Unknown internal error: %v", error),
		)
	}

	defer func(cursor *mongo.Cursor, ctx context.Context) {
		err := cursor.Close(ctx)
		if err != nil {
			fmt.Printf("Error while closing a cursor: %v", err)
		}
	}(cursor, context.Background())

	for cursor.Next(context.Background()) {
		// create an empty struct for response
		data := &postItem{}
		if error := cursor.Decode(data); error != nil {
			return status.Errorf(
				codes.Internal,
				fmt.Sprintf("Error while decoding data from MongoDB: %v", error),
			)
		}

		// send a blog via stream
		if error := stream.Send(&postpb.GetPostsResponse{Post: dataToPostPb(data)}); error != nil {
			return status.Errorf(
				codes.Internal,
				fmt.Sprintf("Error while sending a stream: %v", error),
			)
		}
	}

	if err := cursor.Err(); err != nil {
		return status.Errorf(
			codes.Internal,
			fmt.Sprintf("Unknown internal error: %v", error),
		)
	}

	return nil
}

func (s server) ReadPost(ctx context.Context, request *postpb.ReadPostRequest) (*postpb.ReadPostResponse, error) {
	fmt.Println("Read post request...")

	postId := request.GetPostId()
	oid, error := primitive.ObjectIDFromHex(postId)
	if error != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("Cannot parse ID"),
		)
	}

	// create an empty struct
	data := &postItem{}
	filter := bson.M{"_id": oid} // NewDocument

	// perform find operation
	result := collection.FindOne(ctx, filter)
	if error := result.Decode(data); error != nil {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintf("Cannot find post with specified ID: %v", error),
		)
	}

	// prepare response
	response := &postpb.ReadPostResponse{
		Post: dataToPostPb(data),
	}

	return response, nil
}

func (s server) UpdatePost(ctx context.Context, request *postpb.UpdatePostRequest) (*postpb.UpdatePostResponse, error) {
	fmt.Println("Update blog request...")
	post := request.GetPost()
	oid, error := primitive.ObjectIDFromHex(post.GetId())
	if error != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("Cannot parse ID"),
		)
	}

	// create an empty struct
	data := &postItem{}
	filter := bson.M{"_id": oid}

	result := collection.FindOne(ctx, filter)
	if error := result.Decode(data); error != nil {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintf("Can not find a post with given ID: %v", error),
		)
	}

	// perform update operation
	data.UserID = post.GetUserId()
	data.Title = post.GetTitle()
	data.Body = post.GetBody()

	_, updateError := collection.ReplaceOne(context.Background(), filter, data)
	if updateError != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Can not update object in the Db: %v", updateError),
		)
	}

	// prepare response
	response := &postpb.UpdatePostResponse{
		Post: dataToPostPb(data),
	}

	return response, nil
}

func (s server) DeletePost(ctx context.Context, request *postpb.DeletePostRequest) (*postpb.DeletePostResponse, error) {
	fmt.Println("Delete blog request...")
	oid, error := primitive.ObjectIDFromHex(request.GetPostId())
	if error != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("Cannot parse ID"),
		)
	}

	filter := bson.M{"_id": oid}
	deleteResult, deleteError := collection.DeleteOne(context.Background(), filter)
	if deleteError != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Can not delete object in the Db: %v", deleteError),
		)
	}

	if deleteResult.DeletedCount == 0 {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintf("Can not find blog in the Db: %v", deleteError),
		)
	}

	return &postpb.DeletePostResponse{
		PostId: request.GetPostId(),
	}, nil
}

func dataToPostPb(data *postItem) *postpb.Post {
	return &postpb.Post{
		Id:     data.ID.Hex(),
		UserId: data.UserID,
		Title:  data.Title,
		Body:   data.Body,
	}
}

type postItem struct {
	ID     primitive.ObjectID `json:"_id,omitempty" bson:"_id,omitempty"`
	UserID string             `bson:"user_id"`
	Title  string             `bson:"title"`
	Body   string             `bson:"body"`
}

func main() {
	// if the go code is crushed -> get the file name and line number
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	fmt.Println("Connection to MongoDb")
	// Create client
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}

	// Create connect
	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}

	collection = client.Database("mydb").Collection("posts")

	fmt.Println("Starting Posts GRUD Service...")
	lis, err := net.Listen("tcp", "0.0.0.0:50051")

	if err != nil {
		log.Fatalf("FAILED TO LISTEN %v", err)
	}

	var options []grpc.ServerOption
	s := grpc.NewServer(options...)
	postpb.RegisterPostServiceServer(s, &server{})

	// Register reflection service on gRPC server
	reflection.Register(s)

	go func() {
		fmt.Println("Starting Posts GRUD Server...")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Wait for Control C to exit
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	// Block until a signal is received
	<-ch

	// 1st: close the connection with db
	fmt.Println("Closing MongoDb connection")
	client.Disconnect(context.TODO())

	// 2nd: close the listener
	fmt.Println("Closing the listener")
	lis.Close()

	// Finally, stop the server
	fmt.Println("Stopping the server")
	s.Stop()

	fmt.Println("End of Program")
}
