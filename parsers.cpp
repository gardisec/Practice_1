#include "Head.h"

arr<string> readSchema(const string& filePath) {// ������� ������������ ������ � ������� �� json �����

    json schema;

    ifstream file(filePath);
    if (file.is_open()) {// ������ json ����
        file >> schema;
        file.close();
    }
    else {
        cerr << "�� ������� ������� ����: " << filePath << endl;
    }
    arr<string> jsonSettings;// ��������� ������ ������������ �������

    jsonSettings.push_back(schema["name"]);
    jsonSettings.push_back(",");

    jsonSettings.push_back(toStr(schema["tuples_limit"]));
    jsonSettings.push_back(",");
    
    for (json::iterator it = schema["structure"].begin(); it != schema["structure"].end(); ++it) {// ���� ����������� �������� ������ � ������� � ������ 
        jsonSettings.push_back(it.key());

        for (auto temp : schema["structure"][it.key()]) {
            jsonSettings.push_back(temp);
        }

        jsonSettings.push_back(",");
    }
    return jsonSettings;
}

void request() {// ������� ������� ���������� ������ � ����� �������� ������ �������

    arr<string> path = createCSV();// ��������� ��� ����� � ������������ ��� ���� �� ������ csv ������(���������� ��� �������)
    cout << "< ";
    string tempRequest;
    getline(cin, tempRequest);
    arr<string> commandWords = splitString(" ", tempRequest);// ������� ������ �� ��������, ��� ����������� ����������� �������


    if (commandWords.pointer[0] == "SELECT" && commandWords.pointer[2] == "FROM" && commandWords.pointer[4] == "WHERE") {// ���� ������ � ��������
        try {

            string whereRequest = "";
            for (int i = 5; i < commandWords.currSize; i++) {// ���������� ������ � ������
                whereRequest += commandWords.pointer[i] + " ";
            }
            SelectWhere(commandWords.pointer[3], commandWords.pointer[1], path, whereRequest);
        }
        catch (const exception& error) {
            cerr << error.what() << endl;
        }
    }
    else if (commandWords.pointer[0] == "SELECT" && commandWords.pointer[2] == "FROM"){// ���� ������� ������
        try {

            Select(commandWords.pointer[3], commandWords.pointer[1], path);
            cout << "Select complete" << endl;
        }
        catch (const exception& error) {
            cerr << error.what() << endl;
        }
    }
    else if (commandWords.pointer[0] == "INSERT" && commandWords.pointer[1] == "INTO" && commandWords.pointer[3] == "VALUES" ){ // ���� ������
        try {
            arr<string> data;
            arr<string> temp = splitString("'", tempRequest);// ������� ������ �� ' , ����� ���������� ������ ��� �������

            for (int i = 0; i < temp.currSize; i++) {// ������ ��� ������� ��������� �� �������� � �������� ���������
                if (i % 2 == 1) {
                    data.push_back(temp.pointer[i]);
                }
            }
            Insert(commandWords.pointer[2], data, path);
            cout << "Insert complete" << endl;
        }
        catch (const exception& error) {
            cerr << error.what() << endl;
        }
    }
    else if (commandWords.pointer[0] == "DELETE" && commandWords.pointer[1] == "FROM" && commandWords.pointer[3] == "WHERE" && commandWords.pointer[5] == "=") {// ���� �����
        try {
            string table = commandWords.pointer[2]; // ���������� ������� ��� ������
            string dataFrom = commandWords.pointer[4]; // ������� ��� �������
            
            string filter = splitString("'", tempRequest).pointer[1];// ���������� ������ ��� ��������
            Delete(table, dataFrom, filter, path);
            cout << "Delete complete" << endl;
        }
        catch (const exception& error) {
            cerr << error.what() << endl;
        }
    }
    else if (commandWords.pointer[0] == "EXIT") {// ���� �������� EXIT, �� ����� �� ���������
        exit(1);
    }
    else {
        cout << "Undefined command" << endl;
    }
    
}