import com.amazonaws.util.IOUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.util.*;

public class S3StorageManager
{
    private static final S3StorageManager STORAGE_MANAGER = new S3StorageManager();
    private static final S3Client S3_CLIENT = S3Client.builder().region(Region.US_EAST_2).build();
    private static final Map<String, List<String>> ALL_BUCKETS_AND_OBJECTS = new HashMap<>();

    public static S3StorageManager getStorageManager()
    {
        return STORAGE_MANAGER;
    }

    public String createBucketAndUploadJar(String pathToJar, String fileName) throws IOException {
        String bucket = "bucket" + UUID.randomUUID();

        setUpS3Bucket(bucket, Region.US_EAST_2);

        /*
        Need to add multipart upload here (high level one). The shaded jar to be uploaded to S3 bucket is around
        110 MB, and takes 8 MINUTES to upload. Multipart upload is supposed to speed it up ...
         */

        File f = new File(pathToJar);
        InputStream inputStream = new FileInputStream(f);
        byte[] byteArrayOfFile = IOUtils.toByteArray(inputStream);
        int totalByteLength = byteArrayOfFile.length;

        long startTime = System.currentTimeMillis();

        CreateMultipartUploadRequest multipartUploadRequest = CreateMultipartUploadRequest.builder()
        .bucket(bucket).key(fileName).build();

        CreateMultipartUploadResponse multipartUploadResponse = S3_CLIENT.createMultipartUpload(multipartUploadRequest);
        String uploadID = multipartUploadResponse.uploadId();
        System.out.printf("Multipart Upload ID is: %s\n\n", uploadID);
        List <CompletedPart> completedParts = new ArrayList<>();
        int numberOfParts = 10;
        int eachPartSize = totalByteLength / numberOfParts;
        int residualBytes = totalByteLength - numberOfParts * eachPartSize;
        int partNo = 1;
        int copyWithLoopLength = totalByteLength - residualBytes;
        int i;

        for(i = 0; i < copyWithLoopLength; i += eachPartSize)
        {
            CompletedPart part = completeMultiPartUpload(bucket, fileName, uploadID, partNo, byteArrayOfFile, i, eachPartSize, totalByteLength);
            completedParts.add(part);
            partNo++;
        }

        // this is for residual bytes at the end
        CompletedPart part = completeMultiPartUpload(bucket, fileName, uploadID, partNo, byteArrayOfFile, i, residualBytes+1, totalByteLength);
        completedParts.add(part);

        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                .parts(completedParts).build();

        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucket).key(fileName).uploadId(uploadID).multipartUpload(completedMultipartUpload)
                .build();

        CompleteMultipartUploadResponse completeMultipartUploadResponse = S3_CLIENT.completeMultipartUpload(completeMultipartUploadRequest);

        System.out.println("Total time taken for upload of ~ 111 MB is: " + (System.currentTimeMillis() - startTime));

        String finalLocationUrl = completeMultipartUploadResponse.location();

        /*
        // this is for uploading at 1 go, took a lot of time
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket).key(fileName).build();
        Path path = Paths.get(pathToJar);
        S3_CLIENT.putObject(request, path);
        */

        ALL_BUCKETS_AND_OBJECTS.get(bucket).add(fileName);
        System.out.println("Object was added successfully to the S3 bucket. ");

        return finalLocationUrl;
    }

    private CompletedPart completeMultiPartUpload(String bucket, String fileName, String uploadID, int partNo, byte[] byteArrayOfFile, int i, int eachPartSize, int totalByteLength)
    {
        UploadPartRequest partCopyRequest = UploadPartRequest.builder()
                .bucket(bucket).key(fileName).uploadId(uploadID).partNumber(partNo).build();

        byte[] tempByteArray = Arrays.copyOfRange(byteArrayOfFile, i, i+eachPartSize);
        System.out.println("Size of part to be uploaded is " + tempByteArray.length);
        System.out.println("Range is i: " + i + " and (i+eachPartSize): " + (i+eachPartSize) + ", total size of array: " + totalByteLength);
        String eTag = S3_CLIENT.uploadPart(partCopyRequest, RequestBody.fromBytes(tempByteArray)).eTag();

        CompletedPart part = CompletedPart.builder().partNumber(partNo).eTag(eTag).build();
        System.out.println("Completed part no. " + partNo + " at " + System.currentTimeMillis());
        return part;
    }

    private void setUpS3Bucket(String bucketName, Region region)
    {
        try
        {
            // here region needs to be specified, bucket preferably in same location as emr cluster
            CreateBucketConfiguration configuration = CreateBucketConfiguration.builder()
                    .locationConstraint(region.id()).build();

            // read, write permissions seem to be with this one, setting ACL permission list
            CreateBucketRequest request = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(configuration)
                    .build();
            S3_CLIENT.createBucket(request);
            System.out.printf("Bucket: %s is being created%n", bucketName);

            // waits till s3 bucket actually exists, need something similar for emr cluster creation, some "listener"
            S3_CLIENT.waiter().waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucketName).build());

            ALL_BUCKETS_AND_OBJECTS.put(bucketName, new ArrayList<>());
            System.out.println(bucketName + " has been created and is ready. ");
        }
        catch (Exception e)
        {
            System.out.println("Some exception while s3 bucket creation: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void deleteS3BucketAndContents()
    {
        try
        {
            for(Map.Entry<String, List<String>> s3Bucket : ALL_BUCKETS_AND_OBJECTS.entrySet())
            {
                String bucketName = s3Bucket.getKey();

                for(String object : s3Bucket.getValue())
                {
                    DeleteObjectRequest request = DeleteObjectRequest.builder()
                            .bucket(bucketName).key(object).build();
                    S3_CLIENT.deleteObject(request);

                    System.out.println(object + " object was deleted successfully. ");
                }

                DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                        .bucket(bucketName).build();
                S3_CLIENT.deleteBucket(deleteBucketRequest);

                System.out.println(bucketName + " bucket was deleted successfully. ");
            }
        }
        catch(Exception e)
        {
            System.out.println("Some exception while s3 object / bucket deletion: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("Clean up done. \uD83D\uDC4D ");
    }

}
