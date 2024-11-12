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
public class TitleCrew {

    @Id
    private String tconst;
    private String directors;
    private String writers;

    @ManyToOne
    private TitleBasic titleBasic;
}
