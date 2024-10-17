#pragma once
#include <string>
#include <iostream>
#include <fstream>
#include <ostream>
#include "nlohmann/json.hpp"
#include <filesystem>
#include <stdexcept>
#include <locale>

using namespace std;
namespace fs = std::filesystem;
using json = nlohmann::json;

template <typename T>
struct arr {
    T* pointer;
    size_t currSize; 
    size_t maxSize; 
    arr<T>() : pointer(nullptr), currSize(0), maxSize(0) {}

    void expand();
    void push_back(const T& value);
    void remove(size_t position);
    void printArr();

};

template struct arr<string>;
template struct arr<int>;

void request();
string toStr(const int& value);
int toInt(const string& str);
void createDirectory();
arr<string> splitToArr(const string& input, const string& delimiter);

void unlock(const string& path);
void lock(const string& path);


arr<string> readSchema(const string& filePath);
arr<string> createCSV();

void Insert(const string& table, arr<string>& data, const arr<string>& paths);


void Select(string& fromTable, string SelectData, const arr<string>& paths);

arr<string> splitString(const string value, const string& str);

string tablenameToPath(const string& table, const arr<string>& paths);

void Delete(const string& table, const string& dataFrom, const string& filter, const arr<string>& path);

void SelectWhere(string& fromTable, string SelectData, const arr<string>& paths, string whereRequest);