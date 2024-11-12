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
public class TitleRatings {

    @Id
    private String tconst;
    private float averageRating;
    private int numVotes;

    @ManyToOne
    private TitleBasic titleBasic;
}
