/*
 *
 *  Copyright 2016 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package how.hollow.consumer;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemAnnouncementWatcher;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemBlobRetriever;
import com.netflix.hollow.explorer.ui.jetty.HollowExplorerUIServer;
import com.netflix.hollow.history.ui.jetty.HollowHistoryUIServer;
import how.hollow.consumer.api.generated.MovieAPI;
import java.nio.file.FileSystems;
import java.nio.file.Path;

public class Consumer {
    
    public static void main(String args[]) throws Exception {
        Path publishDir = FileSystems.getDefault().getPath("./publish-dir");
        
        System.out.println("I AM THE CONSUMER.  I WILL READ FROM " + publishDir.toString());

        HollowConsumer.BlobRetriever blobRetriever = new HollowFilesystemBlobRetriever(publishDir);
        HollowConsumer.AnnouncementWatcher announcementWatcher = new HollowFilesystemAnnouncementWatcher(publishDir);
        
        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobRetriever)
                                                .withAnnouncementWatcher(announcementWatcher)
                                                .withGeneratedAPIClass(MovieAPI.class)
                                                .build();
        
        consumer.triggerRefresh();
        
        /// start a history server on port 7777
        HollowHistoryUIServer historyServer = new HollowHistoryUIServer(consumer, 7777);
        historyServer.start();
        
        /// start an explorer server on port 7778
        HollowExplorerUIServer explorerServer = new HollowExplorerUIServer(consumer, 7778);
        explorerServer.start();
        
        historyServer.join();
    }

}
