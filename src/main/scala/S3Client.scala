import java.io.File

import awscala.s3.{Bucket, S3}

object S3Client {

  private val accessKeyId = sys.env.getOrElse("S3_KEY_ID", "AKIAJZUC55ZOLZSRM7RQ")
  private val secretAccessKey = sys.env.getOrElse("S3_KEY_SECRET", "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")

  implicit val region = awscala.Region.US_EAST_1

  implicit val s3 = S3(accessKeyId, secretAccessKey)

  def saveFile(file: File) = {
    val path = Path.dumpFolder + file.getName
    s3.put(Bucket(Buckets.dtlData), path, file)
  }

  def listFiles(bucketName:String) : Vector[String] = s3.objectSummaries(Bucket(bucketName)).map(_.getKey).toVector

  object Buckets {
    val dtlData = "dtl-data"
  }
  object Path{
    val dumpFolder = "PII/dump_2.0/"
  }
}


