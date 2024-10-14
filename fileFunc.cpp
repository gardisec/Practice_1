#include "Head.h"

string toStr(const int& value) { // ������� ����������� ����� � ������
	string result;
	int digit = value;
	while (digit > 0) {
		int temp = digit % 10;
		result = static_cast<char>(temp + '0') + result;
		digit /= 10;
	}
	return result;
}

int toInt(const string& str) { // ������� ����������� ������ � �����
	int result = 0;
	for (int i = 0; i < str.size(); i++) {
		result = result * 10 + static_cast<int>(str[i] - '0');
	}
	return result;
}

arr<string> splitString(const string value, const string& str) {// ������� ����������� ������ �� ��������� �������(������)

	arr<string> result;
	size_t begin = 0;
	size_t end = str.find(value);

	while (end != -1) {
		result.push_back(str.substr(begin, end - begin));
		begin = end + 1;
		end = str.find(value, begin);
	}
	result.push_back(str.substr(begin, end));
	return result;
}

void createDirectory() {// ������� ��������� ���������� �� json �����

	arr<string> settings = readSchema("schema.json");// ������ json
	
	string name = settings.pointer[0];

	if (!fs::exists(name)) { // ���� ��� ����� � ��������� �����, �� ������
		fs::create_directory(name);
	}

	bool isTableName = false;// ���������� ������������ �������� ������ ������ ��������� ������� ��� ���
	for (int i = 3; i < settings.currSize; i++) {
		if (isTableName) { // ���� �������� �������� �� ������� ���������� � ��������� ������ ������� (���� �� ����������)
			if (!fs :: exists(name +  '/'  + settings.pointer[i])) {
				fs::create_directory(name + '/' + settings.pointer[i]);
			}

			isTableName = false;
		}
		if (settings.pointer[i] == ",") { // ����� , ���� �������� ������� ������������� ���������� true
			isTableName = true;
			continue;
		}
			
	}
	
}

arr<string> createCSV() {// �������� ������ � ����������� ����� �� ������������� csv ������

	createDirectory();// ������� ��� ����������
	arr<string> settings = readSchema("schema.json");// ���������� ��� �����, ����� �����, �������� ������� � ������
	string name = settings.pointer[0];
	const int tuple_limit = toInt(settings.pointer[2]);

	arr<string> pathToFiles; 
	string currPath, pathPKFile, pathLockFile, tempLine;
	int counter, j;
	int fileIndex = 1;
	bool isTableName = false;

	for (int i = 3; i < settings.currSize; i++) {
		if (settings.pointer[i] == ",") {// ���� ��� , �� ���� ������ 
			isTableName = true;
			continue;
		}
		else if (isTableName) {
			while (1) {// ���� ������������ ������������� csv ����

				currPath = name + '/' + settings.pointer[i] + '/' + toStr(fileIndex) + ".csv"; //���������� ���� �� csv �����
				counter = 0;
				ifstream tuplLimit(currPath);

				while (getline(tuplLimit, tempLine)) {// ������� ���-�� ����� � ���
					
					counter++;
					
					
				}
				if (counter < tuple_limit + 1) { // ���� ����� � ����� ������ ��� ����� �� ������ ������������ ������ �� ��������� ��������� ����(���� ��� �� ������� ���)
					pathToFiles.push_back(currPath);
					break;
				}
				else {
					fileIndex++;
					continue;
				}
				
			}
			

			pathPKFile = name + '/' + settings.pointer[i] + '/' + settings.pointer[i] + "_pk_sequence.txt";// �������� pk_sequence.txt ����� � ���������� ��� ���������� �������
			if (!fs::exists(pathPKFile)) {// ������� pk - �����
				ofstream pkFile(pathPKFile);
				pkFile << "0";
				pkFile.close();
			}

			pathLockFile = name + '/' + settings.pointer[i] + '/' + settings.pointer[i] + "_lock.txt";// �������� lock.txt ����� � ���������� ��� ���������� �������
			if (!fs::exists(pathLockFile)) {// ������� pk - �����
				ofstream lockFile(pathLockFile);
				lockFile << "0";
				lockFile.close();
			}
			j = i + 1;

			if (!fs::exists(currPath)) {// ���������� ������������ ������ � csv ������
				ofstream file(currPath);
				file << settings.pointer[i] << "_pk,";
				while (settings.pointer[j] != ",") {
					file << settings.pointer[j];
					if (settings.pointer[j + 1] != ",") {
						file << ",";
					}

					j++;
				}
				file << "\n";
				file.close();
			}
			isTableName = false;
		}

	}

	return pathToFiles; 
}

