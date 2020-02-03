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
package how.hollow.producer;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemAnnouncementWatcher;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemBlobRetriever;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.HollowProducer.Announcer;
import com.netflix.hollow.api.producer.HollowProducer.Publisher;
import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemPublisher;

import how.hollow.consumer.api.generated.MovieAPI;
import how.hollow.producer.datamodel.Actor;
import how.hollow.producer.datamodel.Movie;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class IncrementalProducer {
    
    public static void main(String args[]) {
        Path publishDir = FileSystems.getDefault().getPath("./publish-dir");
        
        System.out.println("I AM THE INCREMENTAL PRODUCER.  I WILL PUBLISH TO " + publishDir.toString());

        HollowConsumer.BlobRetriever blobRetriever = new HollowFilesystemBlobRetriever(publishDir);
        HollowConsumer.AnnouncementWatcher announcementWatcher = new HollowFilesystemAnnouncementWatcher(publishDir);
        
        HollowConsumer consumer = HollowConsumer.withBlobRetriever(blobRetriever)
                                                .withAnnouncementWatcher(announcementWatcher)
                                                .withGeneratedAPIClass(MovieAPI.class)
                                                .build();
        
        consumer.triggerRefresh();
        
        Publisher publisher = new HollowFilesystemPublisher(publishDir);
        Announcer announcer = new HollowFilesystemAnnouncer(publishDir);
        
       
        HollowProducer.Incremental producer = HollowProducer.withPublisher(publisher).withAnnouncer(announcer)
                .buildIncremental();
        
        producer.initializeDataModel(Movie.class);
        
        producer.restore(consumer.getCurrentVersionId(), blobRetriever);        

        Actor actor1 = new Actor("one", "Gary", new ArrayList<>());
        actor1.tags.add("tag1");
        List<Actor> set1 = new ArrayList<>();
        set1.add(actor1);
        Movie movie1 = new Movie("one", "Movie1v2", set1);
        
        producer.runIncrementalCycle(incrementalWriteState -> {
        	incrementalWriteState.addOrModify(movie1);
        });
    }

}
