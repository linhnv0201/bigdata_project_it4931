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
public class TitlePrincipals {

    @Id
    private String tconst;
    private int ordering;
    private String nconst;
    private String category;
    private String job;
    private String characters;

    @ManyToOne
    private TitleBasic titleBasic;
}

