package com.example.demo_bigdata_imdb.entity;


import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter @Setter @AllArgsConstructor @NoArgsConstructor
public class TitleEpisode {

    @Id
    private String tconst;
    private String parentTconst;
    private int seasonNumber;
    private int episodeNumber;

    @ManyToOne
    private TitleBasic titleBasic;
}

