package main

import (
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	Profile        = "aws_s3_access_profile"
	BucketName     = "BucketName"
	ObjectKey      = "sampleFolder/sample.txt"
	TargetFilePath = "./sample.txt"
	DownloadFile   = "downloadInput.txt"
)

// s3AccessData構造体はAWS S3へのアクセスに必要な情報を保持する
// これには、S3バケットの名前とアクセスするオブジェクトのキーが含まれる
type s3AccessData struct {
	bucketName string
	objectkey  string
}

func main() {
	// セッションを作成
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Profile:           Profile,
		SharedConfigState: session.SharedConfigEnable,
	}))

	s3AccessInput := s3AccessData{
		BucketName,
		ObjectKey,
	}

	// データをS3バケットにアップロード
	uploadDataToS3(sess, s3AccessInput)

	// データが正常にアップロードされることを確認するために遅延を追加
	time.Sleep(2 * time.Second)

	// アップロードしたデータをS3バケットからダウンロード
	downloadDataFromS3(sess, s3AccessInput)

	// S3バケットからダウンロードしたデータを読み込む
	loadDataFromS3(sess, s3AccessInput)
}

// uploadDataToS3はAWS S3にファイルをアップロードする関数
// セッションとS3アクセスデータをパラメータとして受け取る
func uploadDataToS3(sess *session.Session, s3AccessData s3AccessData) {
	file, err := os.Open(TargetFilePath)
	checkError(err)
	defer file.Close()

	// 新しいアップローダーインスタンスを作成
	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3AccessData.bucketName),
		Key:    aws.String(s3AccessData.objectkey),
		Body:   file,
	})
	checkError(err)
}

// downloadDataFromS3はAWS S3からファイルをダウンロードする関数
// セッションとS3アクセスデータをパラメータとして受け取る
func downloadDataFromS3(sess *session.Session, s3AccessData s3AccessData) {
	file, err := os.Create(DownloadFile)
	checkError(err)

	// 新しいダウンローダーインスタンスを作成
	downloader := s3manager.NewDownloader(sess)
	n, err := downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(s3AccessData.bucketName),
		Key:    aws.String(s3AccessData.objectkey),
	})
	checkError(err)

	log.Printf("DownloadedSize: %d byte", n)
}

// loadDataFromS3はAWS S3からファイルを読み込み、読み取る関数
// セッションとS3アクセスデータをパラメータとして受け取る
func loadDataFromS3(sess *session.Session, s3AccessData s3AccessData) {
	svc := s3.New(sess)

	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3AccessData.bucketName),
		Key:    aws.String(s3AccessData.objectkey),
	})
	checkError(err)

	rc := obj.Body
	defer rc.Close()

	buf := make([]byte, 20)
	_, err = rc.Read(buf)
	checkError(err)
	log.Printf("%s", buf)
}

// checkErrorはエラーをチェックし、エラーが存在する場合にはログに記録する
func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
