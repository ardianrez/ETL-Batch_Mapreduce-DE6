import functions as fx


if __name__ == "__main__":
    #insert data ke database digitalskola
    fx.insert_data()

    #ingest data dari database ke hadoop
    fx.ingestion_data()

    #transform data di hadoop
    fx.transform_data()

    #menggunakan mapreduce
    
    #proses mapping
    fx.mapper()

    #proses reduce
    fx.reducer()
