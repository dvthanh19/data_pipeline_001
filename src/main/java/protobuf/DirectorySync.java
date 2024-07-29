package protobuf;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;


public class DirectorySync {
    public static void main(String[] args) {
        String path = "D:\\dir_client";
        DirectoryWatcher.dir_watch(path);
    }
}

class DirectoryWatcher {
    public static void dir_watch(String path) {
        Path directoryToWatch = Paths.get(path);

        try (WatchService watchService = directoryToWatch.getFileSystem().newWatchService()) {
            // Register the directory with the WatchService
            directoryToWatch.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);

            System.out.println("Watch service registered for directory: " + directoryToWatch.toAbsolutePath());


            // Infinite loop to keep the program running
            while (true) {
                WatchKey key;
                try {
                    // Retrieve and remove the next watch key
                    key = watchService.take();
                } catch (InterruptedException ex) {
                    System.out.println("InterruptedException caught.");
                    return;
                }

                // Process events for the key
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    // This key is no longer valid, exit loop
                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }

                    // The filename is the context of the event
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path filename = ev.context();

                    // Print the event details
                    System.out.printf("Event kind: %s. File affected: %s.%n", kind.name(), filename);

                    // Main_flow.dir_processing();
                }




                // Reset the key -- this step is critical if you want to receive further events
                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }
        } catch (IOException ex) {
            System.out.println("IOException caught: " + ex.getMessage());
        }
    }
}
