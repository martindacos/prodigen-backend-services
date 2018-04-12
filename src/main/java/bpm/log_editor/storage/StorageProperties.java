package bpm.log_editor.storage;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("storage")
public class StorageProperties {

    /**
     * Folder location for storing files
     */
    private static String location = "upload-dir";

    public static String getLocation() {
        return location;
    }

    public static void setLocation(String location) {
        StorageProperties.location = location;
    }

}
