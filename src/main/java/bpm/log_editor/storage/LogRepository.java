package bpm.log_editor.storage;

import bpm.log_editor.data_types.LogFile;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface LogRepository extends MongoRepository <LogFile, String>{

    public LogFile findByName(String name);

}
