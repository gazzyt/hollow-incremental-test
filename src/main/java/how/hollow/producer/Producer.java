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

import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.HollowProducer.Announcer;
import com.netflix.hollow.api.producer.HollowProducer.Publisher;
import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemPublisher;

import how.hollow.producer.datamodel.Actor;
import how.hollow.producer.datamodel.Movie;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;


public class Producer {
    
    public static void main(String args[]) {
        Path publishDir = FileSystems.getDefault().getPath("./publish-dir");
        
        System.out.println("I AM THE PRODUCER.  I WILL PUBLISH TO " + publishDir.toString());
        
        Publisher publisher = new HollowFilesystemPublisher(publishDir);
        Announcer announcer = new HollowFilesystemAnnouncer(publishDir);
        
        HollowProducer producer = HollowProducer.withPublisher(publisher)
                                                .withAnnouncer(announcer)
                                                .build();
        
        producer.initializeDataModel(Movie.class);
        
        Actor actor1 = new Actor(1, "Gary");
        Set<Actor> set1 = new HashSet<>();
        set1.add(actor1);
        Actor actor2 = new Actor(1, "Gary");
        Set<Actor> set2 = new HashSet<>();
        set2.add(actor2);
        Movie movie1 = new Movie(1, "Movie1", set1);
        Movie movie2 = new Movie(2, "Movie2", set2);
        
        producer.runCycle(writeState -> {
            writeState.add(movie1);
            writeState.add(movie2);
        });

    }

}
