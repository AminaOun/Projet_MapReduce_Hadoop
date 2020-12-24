package ZIP;


public class UnzipUtilityTest {
    public static void main(String[] args) {
        String zipFilePath = "C:/Users/ounja/Downloads/audio-learning-master.zip";
        String destDirectory = "C:/Users/ounja/Desktop/A/";
        UnzipUtility unzipper = new UnzipUtility();
        try {
            unzipper.unzip(zipFilePath, destDirectory);
        } catch (Exception ex) {
            // some errors occurred
            ex.printStackTrace();
        }
        System.out.println("unzip done successfuly !!");
    }
}