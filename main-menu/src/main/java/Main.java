import java.io.IOException;
import java.util.Objects;
import java.util.Scanner;

public class Main
{
    public static void main(String[] args) throws IOException {
        System.out.println("Starting main procedure ...\n");
        int input = 1;
        int clusterCount = 0;
        Scanner sc = new Scanner(System.in);
        String s3JarLocation = null;
        while(input != 0)
        {
            printMenu();
            input = sc.nextInt();
            System.out.println("\n");
            switch (input)
            {
                case 1:
                    if(clusterCount == 1)
                    {
                        System.out.println("1 cluster already launched ... can't afford another ");
                    }
                    else
                    {
                        clusterCount++;
                        SparkClusterManager.getSparkClusterManager().startCluster();
                    }
                    break;
                case 2:
                    String pathToJar = "F:\\spark-sample\\target\\spark-sample-1.0-SNAPSHOT.jar";
                    String fileName = "spark-sample-1.0-SNAPSHOT.jar";
                    s3JarLocation = S3StorageManager.getStorageManager().createBucketAndUploadJar(pathToJar, fileName);
                    break;
                case 3:
                    if(clusterCount < 1)
                    {
                        System.out.println("Cluster hasn't been initialised yet ");
                    }
                    else if(Objects.nonNull(s3JarLocation))
                    {
                        SparkClusterManager.getSparkClusterManager().runJob(s3JarLocation);
                    }
                    break;
                case 4:
                    input = 0;
                    if(clusterCount > 0)
                    {
                        SparkClusterManager.getSparkClusterManager().terminateCluster();
                    }
                    // for poc purpose, commenting out releasing s3 bucket resources
                    // S3StorageManager.getStorageManager().deleteS3BucketAndContents();
                    break;
                default:
                    System.out.println("Enter proper choice. ");
            }
        }
    }

    public static void printMenu()
    {
        String message = "Your options are :-\n" +
                "\n" +
                "1) Launch the AWS EMR cluster (only 1 allowed)\n" +
                "\n" +
                "2) Create the S3 bucket and upload input, jar files\n" +
                "\n" +
                "3) Submit job to AWS EMR cluster\n" +
                "\n" +
                "4) Terminate the AWS EMR cluster & Delete the S3 bucket and contents\n" +
                "\n" +
                "Enter corresponding option to enact that action ...\n";
        System.out.println(message);
    }
}
