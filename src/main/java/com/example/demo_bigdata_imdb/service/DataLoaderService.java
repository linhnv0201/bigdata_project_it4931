package com.example.demo_bigdata_imdb.service;

import com.example.demo_bigdata_imdb.entity.TitleBasic;
import com.example.demo_bigdata_imdb.repository.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;


@Service
public class DataLoaderService {

    @Autowired
    private TitleBasicRepository titleBasicRepository;
    @Autowired
    private TitleAkasRepository titleAkasRepository;
    @Autowired
    private TitleCrewRepository titleCrewRepository;
    @Autowired
    private TitleEpisodeRepository titleEpisodeRepository;
    @Autowired
    private TitlePrincipalsRepository titlePrincipalsRepository;
    @Autowired
    private TitleRatingsRepository titleRatingsRepository;
    @Autowired
    private NameBasicsRepository nameBasicsRepository;

    public void loadDataTitleFromTSV(String filePath) throws IOException {
        // Đọc dữ liệu từ file .tsv
        FileReader fileReader = new FileReader(filePath);
        Iterable<CSVRecord> records = CSVFormat.TDF.withHeader().parse(fileReader);

        for (CSVRecord record : records) {
            // TitleBasics
            TitleBasic titleBasic = new TitleBasic();
            titleBasic.setTconst(record.get("tconst"));
            titleBasic.setTitleType(record.get("titleType"));
            titleBasic.setPrimaryTitle(record.get("primaryTitle"));
            titleBasic.setOriginalTitle(record.get("originalTitle"));
            titleBasic.setAdult("1".equals(record.get("isAdult")));
            titleBasic.setStartYear(Integer.parseInt(record.get("startYear")));
            titleBasic.setEndYear(Integer.parseInt(record.get("endYear")));
            titleBasic.setRuntimeMinutes(Integer.parseInt(record.get("runtimeMinutes")));
            titleBasic.setGenres(record.get("genres"));
            titleBasicRepository.save(titleBasic);

            // Tiếp tục lưu các bảng khác (title_akas, title_crew, etc.)
        }
    }
}

