/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ndemyanovskyi.derby;

import com.ndemyanovskyi.throwable.Exceptions;
import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import org.apache.derby.jdbc.EmbeddedDriver;

public final class Derby {

    private static WeakReference<Driver> driver;

    public static Database connect(String path) {
        Objects.requireNonNull(path, "path");

        return Exceptions.execute(() -> {
            loadDriver();

            Connection c = DriverManager.getConnection(
                    "jdbc:derby:" + path + ";create=true;", "", "");
            return new Database(path, c);
        });
    }

    public static Database connect(String path, String username, String password) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(password, "password");

        return Exceptions.execute(() -> {
            loadDriver();

            Connection c = DriverManager.getConnection(
                    "jdbc:derby:" + path + ";create=true;", username, password);
            return new Database(path, c);
        });
    }

    private static void loadDriver() throws SQLException {
        if(driver == null || driver.get() == null) {
            driver = new WeakReference<>(new EmbeddedDriver());
            DriverManager.registerDriver(driver.get());
        }
    }

}
