package org.example;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor

public class Job {
    private String jobId;
    private String jobTitle;
    private String jobDescription;
    private String searchableContent;
    private String eventId;
}
