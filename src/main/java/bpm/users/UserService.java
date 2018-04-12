package bpm.users;

import bpm.log_editor.data_types.LogFile;
import bpm.log_editor.storage.StorageService;

import java.util.ArrayList;

public class UserService {

    private static UserRepository repo;
    public static StorageService storageService;
    private static ArrayList<User> users = new ArrayList<>();

    public static void init(UserRepository repository) {
        //Init repo
        repo = repository;

        //Select all files from collection
        users = (ArrayList<User>) repo.findAll();
    }

    public static LogFile insertUser(String name, String path) {
        //Insert log
        repo.save(new User());

        //Return created log
        return null;
    }

    public static ArrayList<User> getUsers() {
        return users = (ArrayList<User>) repo.findAll();
    }

}
