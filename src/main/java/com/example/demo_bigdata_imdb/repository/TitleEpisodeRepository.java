package com.example.demo_bigdata_imdb.repository;

import com.example.demo_bigdata_imdb.entity.TitleEpisode;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TitleEpisodeRepository extends JpaRepository<TitleEpisode, String> {}
