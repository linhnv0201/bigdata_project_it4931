package com.example.demo_bigdata_imdb.repository;

import com.example.demo_bigdata_imdb.entity.TitleRatings;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TitleRatingsRepository extends JpaRepository<TitleRatings, String> {}
