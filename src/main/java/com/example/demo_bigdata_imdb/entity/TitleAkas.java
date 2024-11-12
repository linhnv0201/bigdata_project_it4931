package com.example.demo_bigdata_imdb.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class TitleAkas {

    @Id
    private String titleId;
    private int ordering;
    private String title;
    private String region;
    private String language;
    private String types;
    private String attributes;
    private boolean isOriginalTitle;

    @ManyToOne
    private TitleBasic titleBasic;  // Liên kết với bảng title_basics
}

