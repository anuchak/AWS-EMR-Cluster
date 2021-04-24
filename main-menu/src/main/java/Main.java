import java.util.Scanner;

public class Main
{
    public static void main(String[] args)
    {
        System.out.println("Starting main procedure ...\n");
        int input = 1;
        int clusterCount = 0;
        Scanner sc = new Scanner(System.in);
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
                    break;
                case 3:
                    if(clusterCount < 1)
                    {
                        System.out.println("Cluster hasn't been initialised yet ");
                    }
                    else
                    {
                        SparkClusterManager.getSparkClusterManager().runJob();
                    }
                    break;
                case 4:
                    input = 0;
                    SparkClusterManager.getSparkClusterManager().terminateCluster();
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
                "4) Terminate the AWS EMR cluster\n" +
                "\n" +
                "Enter corresponding option to enact that action ...\n";
        System.out.println(message);
    }
}
