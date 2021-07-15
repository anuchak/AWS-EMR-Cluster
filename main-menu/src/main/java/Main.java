import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

public class Main
{
    public static void main(String[] args) throws IOException {
        System.out.println("Starting main procedure ...\n");
        int input = 1;
        int clusterCount = 0;
        Scanner sc = new Scanner(System.in);
        File f = new File("ignite.json");
        TypeReference <java.util.Map<String, String>> typeRef = new TypeReference<Map<String, String>>() {};
        Map <String, String> properties = new ObjectMapper().readValue(f, typeRef);
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
                        SparkClusterManager.getSparkClusterManager().startCluster(properties);
                    }
                    break;
                case 2:
                    String pathToFile = takeInput("Path to Object to be uploaded");
                    String fileName = takeInput("File Name in S3 bucket");
                    String s3ObjectLocation = S3StorageManager.getStorageManager().createBucketAndUploadJar(pathToFile, fileName);
                    System.out.println("S3 location to which object uploaded is: " + s3ObjectLocation);
                    break;
                case 3:
                    if(clusterCount < 1)
                    {
                        System.out.println("Cluster hasn't been initialised yet ");
                    }
                    SparkClusterManager.getSparkClusterManager().runJob(properties);
                    break;
                case 4:
                    input = 0;
                    if(clusterCount > 0)
                    {
                        SparkClusterManager.getSparkClusterManager().terminateCluster();
                    }
                    // for poc purpose, commenting out releasing s3 bucket resources
                    S3StorageManager.getStorageManager().deleteS3BucketAndContents();
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

    public static String takeInput(String entity)
    {
        System.out.println("Give input for " + entity);
        Scanner sc = new Scanner(System.in);
        String input = sc.nextLine();
        System.out.println("Input provided by user: " + input);
        return input;
    }

}
