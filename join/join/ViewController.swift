//
//  ViewController.swift
//  join
//
//  Created by Droadmin on 08/08/23.
//

import UIKit
import SQLite3
class ViewController: UIViewController {
    var db: OpaquePointer? = nil

    @IBOutlet weak var markTxt: UITextField!
    @IBOutlet weak var nameTxt: UITextField!
    override func viewDidLoad() {
        super.viewDidLoad()
        db = createDb()
        nameTbl()
        markTbl()
        
        print("---------------Inner Join---------------")
        innerJoinAndPrintData()
        print("---------------Left Join----------------")
        leftJoin()
        print("---------------Full Join----------------")
        fullJoin()
        
        // Do any additional setup after loading the view.
    }
    
    @IBAction func submitBtn(_ sender: Any) {
        insertNameData(name: nameTxt.text ?? "")
        insertMarkData(mark: markTxt.text ?? "")
    }
    func createDb() ->OpaquePointer?{
        let fileURL = try! FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create:false).appendingPathComponent("join.db")
        if sqlite3_open(fileURL.path, &db) == SQLITE_OK{
            print("\(fileURL)")
            return db
        }else{
            print("failed to open")
            return nil
        }
        
    }
    func nameTbl(){
        let query = "CREATE TABLE IF NOT EXISTS student(Id INTEGER PRIMARY KEY,name TEXT);"
        var statemet : OpaquePointer? = nil
        if sqlite3_prepare_v2(db, query, -1, &statemet, nil) == SQLITE_OK{
            if sqlite3_step(statemet) == SQLITE_DONE{
                print("name table creted")
            }else{
                print("not creted")
            }
            sqlite3_finalize(statemet)
        }
    }
    func markTbl(){
        let query = "CREATE TABLE IF NOT EXISTS mark(Id INTEGER PRIMARY KEY,mark TEXT);"
        var statemet : OpaquePointer? = nil
        if sqlite3_prepare_v2(db, query, -1, &statemet, nil) == SQLITE_OK{
            if sqlite3_step(statemet) == SQLITE_DONE{
                print("mark table creted")
            }else{
                print("not creted")
            }
            sqlite3_finalize(statemet)
        }
    }
    func insertNameData(name:String){
        let query = "INSERT INTO student (name) VALUES (?);"
        var statement: OpaquePointer?
        if sqlite3_prepare_v2(db, query, -1 , &statement, nil) == SQLITE_OK{
            sqlite3_bind_text(statement, 1,(name as NSString) .utf8String, -1, nil)
            
            if sqlite3_step(statement) == SQLITE_DONE{
                print("data inserted succecfully")
            }else{
                print("failed query")
            }
            sqlite3_finalize(statement)
        }
        
    }
    func insertMarkData(mark:String){
        let query = "INSERT INTO mark (mark) VALUES (?);"
        var statement: OpaquePointer?
        if sqlite3_prepare_v2(db, query, -1 , &statement, nil) == SQLITE_OK{
            sqlite3_bind_text(statement, 1,(mark as NSString) .utf8String, -1, nil)
            
            if sqlite3_step(statement) == SQLITE_DONE{
                print("data inserted succecfully")
            }else{
                print("failed query")
            }
            sqlite3_finalize(statement)
        }
        
    }
    func innerJoinAndPrintData() {
        let query = "SELECT * FROM student INNER JOIN mark ON student.Id = mark.Id;"
        
        var statement: OpaquePointer?
        
        if sqlite3_prepare_v2(db, query, -1, &statement, nil) == SQLITE_OK {
            while sqlite3_step(statement) == SQLITE_ROW {
                let studentId = Int(sqlite3_column_int(statement, 0))
                if let studentNamePtr = sqlite3_column_text(statement, 1){
                    let studentName = String(cString: studentNamePtr)
                    print(" StudentID InnerJoin:\(studentId), Student Name: \(studentName)")
                }else{
                    print("Student name nill")
                }
                let markId = Int(sqlite3_column_int(statement, 2))
                
                if let markPtr = sqlite3_column_text(statement, 3) {
                    
                    let mark = String(cString: markPtr)
                    print(" markID InnerJoin:\(markId), Student mark: \(mark)")
                } else {
                    print("Mark nill")
                }
            }
            
            sqlite3_finalize(statement)
        } else {
            print("Error preparing statement for INNER JOIN: \(String(cString: sqlite3_errmsg(db)))")
        }
    }
    func leftJoin(){
        let query = "SELECT * FROM student LEFT OUTER JOIN mark ON student.id = mark.id;"
        
        var statement: OpaquePointer?
        
        if sqlite3_prepare_v2(db, query, -1, &statement, nil) == SQLITE_OK {
            while sqlite3_step(statement) == SQLITE_ROW {
                let Id = Int(sqlite3_column_int(statement, 0))
                if let studentNamePtr = sqlite3_column_text(statement, 1){
                    let studentName = String(cString: studentNamePtr)
                    print(" ID LEFTJOIN:\(Id), Student Name: \(studentName)")
                }else{
                    print("Student nill")
                }
                let markId = Int(sqlite3_column_int(statement, 2))
                if let markPtr = sqlite3_column_text(statement, 3) {
                    
                    let mark = String(cString: markPtr)
                    print(" ID LEFTJOIN:\(markId), Student mark: \(mark)")
                } else {
                    print("Mark nill")
                }
                // print("Student ID Left Join: \(studentId), Student Name: \(studentName), Mark: \(mark)")
                
            }
            
            sqlite3_finalize(statement)
        } else {
            print("Error preparing statement for LEFT JOIN: \(String(cString: sqlite3_errmsg(db)))")
        }
        
    }
    func fullJoin(){
        let query = "SELECT * FROM student FULL JOIN mark ON student.id = mark.id;"
        var statement: OpaquePointer?
        
        if sqlite3_prepare_v2(db, query, -1, &statement, nil) == SQLITE_OK {
            while sqlite3_step(statement) == SQLITE_ROW {
                let studentId = Int(sqlite3_column_int(statement, 0))
                if let studentNamePtr = sqlite3_column_text(statement, 1){
                    let studentName = String(cString: studentNamePtr)
                    print(" StudentID fullJoin:\(studentId), Student Name: \(studentName)")
                }else{
                    print("Student name nill")
                }
                let markId = Int(sqlite3_column_int(statement, 2))
                
                if let markPtr = sqlite3_column_text(statement, 3) {
                    
                    let mark = String(cString: markPtr)
                    print(" markID fullJoin:\(markId), Student mark: \(mark)")
                } else {
                    print("Mark nill")
                }
            }
            
            sqlite3_finalize(statement)
        } else {
            print("Error preparing statement for INNER JOIN: \(String(cString: sqlite3_errmsg(db)))")
        }
    }
}




