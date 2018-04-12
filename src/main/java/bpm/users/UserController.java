package bpm.users;

import bpm.log_editor.parser.CSVparser;
import bpm.log_editor.storage.LogService;
import bpm.log_editor.storage.StorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;


@RestController
public class UserController {

    private static final String template = "Your file is, %s!";
    private final AtomicLong counter = new AtomicLong();

    private final StorageService storageService;

    @Autowired
    public UserController(StorageService storageService) {
        this.storageService = storageService;
        CSVparser.storageService = storageService;
        LogService.storageService = storageService;
    }

    //--USER INFO--//
    //Returns log stats about a user
    @CrossOrigin
    @RequestMapping(value = "/bpm/users/{id:.+}", method = RequestMethod.GET)
    public void getUserInfo(@PathVariable("id") String id) throws IOException {
        LogService.deleteLog(LogService.getLogByName(id));
    }
}
