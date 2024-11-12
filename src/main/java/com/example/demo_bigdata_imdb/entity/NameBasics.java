package com.example.demo_bigdata_imdb.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class NameBasics {

    @Id
    private String nconst;
    private String primaryName;
    private int birthYear;
    private int deathYear;
    private String primaryProfession;
    private String knownForTitles;
}

