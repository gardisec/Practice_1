#include "Head.h"


string tablenameToPath(const string& table, const arr<string>& paths) {// ������� ������� ���������� ���� �� ������� csv ����� �� �������� �������

	string correctPath;
	arr<string> tempArray;
	for (int i = 0; i < paths.currSize; i++) {// �������� �� ���� �����

		tempArray = splitString("/", paths.pointer[i]); // ������� �� "/"

		for (int j = 0; j < tempArray.currSize; j++) {//���� �������� ������� ��������� �� ������� ����
			if (table == tempArray.pointer[j]) {
				correctPath = paths.pointer[i];
				return correctPath;
			}
		}
	}
}


void Insert(const string& table, arr<string>& data, const arr<string>& paths) {

	
	const int tuple_limit = toInt(readSchema("schema.json").pointer[2]);// ��������� ������ � ����� json
	string correctPath = tablenameToPath(table, paths);
	string nameSchema = readSchema("schema.json").pointer[0];

	string tempString, pk;
	ifstream lockFile(nameSchema + '/' + table + '/' + table + "_lock.txt");//�������� �� ���������� �����
	getline(lockFile, tempString);
	lockFile.close();

	if (tempString == "0") {// ���� ������������� ����, �� �������� ������ � ���
		
		ofstream lockFile(nameSchema + '/' + table + '/' + table + "_lock.txt");// ���������� �����
		lockFile << "1";
		lockFile.close();
		string line;
		ifstream file(correctPath);//������ ������ �������
		if (file.is_open()) {
			getline(file, line);
			file.close();
		}
		arr<string> maxColums = splitString(",", line);//������� �������� �� , � ����� ���������� � � �������

		if (maxColums.currSize - 1 == data.currSize) {// ���� ���-�� ������ � ������� ��������� ������������� ���-�� ������ � ������ �����
			
			ifstream pkCount(nameSchema + '/' + table + '/' + table + "_pk_sequence.txt");//������� ����� �������
			getline(pkCount, pk);
			pkCount.close();

			
			ofstream file(correctPath, ios::ate | ios::app);//���������� ����� ������� � ������
			file << pk << ",";
			for (int j = 0; j < data.currSize; j++) {
				file << data.pointer[j];
				if (j != data.currSize - 1) {
					file << ",";
				}
			}
			file << "\n";
			file.close();
		}
		else { 
			cerr << "Incorrect number of elements" << endl;
		}
		
		ofstream unlockFile(nameSchema + '/' + table + '/' + table + "_lock.txt");// ��������������� �����
		unlockFile << "0";
		unlockFile.close();

		ofstream pkFileRecord(nameSchema + '/' + table + '/' + table + "_pk_sequence.txt");//�������������� pk � ���������� � ����
		pkFileRecord << toInt(pk) + 1;
		pkFileRecord.close();


	}
	else if (tempString == "1") {//���� ��� ������ ��� ��������, �� �������
		
		cerr << "File already is open. Try again letter." << endl;
	}
	
}//INSERT INTO table2 VALUES ('zxczxb', 'asdasdg')

void Select(string& fromTable, string SelectData, const arr<string>& paths) {

	arr<string> allChose = splitString(",", SelectData);// ��������� ������ 
	arr<string> temp;
	arr<string> empty;
	arr<string> splitedChose;

	for (int i = 0; i < allChose.currSize; i++) {// �� ������ �������� ������ ������� , ������� ; ���� ������ �������� , ������� 
		temp = splitString(".", allChose.pointer[i]);
		for (int j = 0; j < temp.currSize; j++) {
			splitedChose.push_back(temp.pointer[j]);
		}

	}
	temp = empty;//������� �������
	int numColumn, count;
	string currentPath, column, stringFromFile;
	arr<string> tables = splitString(",", fromTable);// ������ � ���������� ������ � ������� ��������
	for (int i = 0; i < tables.currSize; i++) {

		currentPath = tablenameToPath(tables.pointer[i], paths);//���������� ���� �� .csv ����� �� ������� ������� 


		for (int j = 0; j < splitedChose.currSize; j++) {
			count = 0;
			if (tables.pointer[i] == splitedChose.pointer[j]) {// ���� �������� ������� ���������, �� ��������� ������� � ������� ��� ������� � ������� ����������
				column = splitedChose.pointer[j + 1];
				numColumn = column[column.size() - 1] - 48;// ������� ����� ������� � ������� ����������

			}
			else {
				continue;
			}

			ifstream file(currentPath);

			if (file.is_open()) {
				while (getline(file, stringFromFile)) {// ��������� �������
					if (file.eof()) {// ���� ����� ����� �� �������
						file.close();
						break;
					}
					if (count == 0) {//���� ������� ������ � ����� �� ����������
						count++;
						continue;
					}
					else {//��������� � ������ ����� ������� � ������� ������� ����������� � ������ ������� ����� �������
						temp.push_back(splitString(",", stringFromFile).pointer[0] + "," + splitString(",", stringFromFile).pointer[numColumn]);
					}

				}
				temp.push_back(",");// ������ �������, ��� ������������ ������ ������ �� 1 �������
			}


		}
	}

	arr<string> indexTemp;
	for (int i = 0; i < temp.currSize; i++) {// ��������� ������ � ��������� ��������� ������ ������ �������
		if (temp.pointer[i] == ",") {
			indexTemp.push_back(toStr(i));
		}
	}
	arr<string> left, right;

	for (int i = 0; i < toInt(indexTemp.pointer[0]); i++) {//������ ����� ��� ���� �������
		for (int j = toInt(indexTemp.pointer[0]) + 1; j < toInt(indexTemp.pointer[1]); j++) {//�������� �� ������ �������
			left = splitString(",", temp.pointer[i]);
			right = splitString(",", temp.pointer[j]);
			cout << " \t" << left.pointer[0] << "   " << left.pointer[1] << " \t" << right.pointer[0] << "   " << right.pointer[1] << endl;
		}
	}

}
//SELECT table2.column1,table2.column2 FROM table2

void Delete(const string& table,const string& dataFrom,const string& filter,const arr<string>& path) {
	string currentPath = tablenameToPath(table, path);// ���������� ���� �� ����� ��� ������� ��������

	string nameSchema = readSchema("schema.json").pointer[0];// ���������� ��� �����
	string tempString;

	ifstream lockFile(nameSchema + '/' + table + '/' + table + "_lock.txt");// �������� ���� ����������
	getline(lockFile, tempString);
	lockFile.close();

	if (tempString == "0") {// ���� ��������������, �� ��������

		ofstream lockFile(nameSchema + '/' + table + '/' + table + "_lock.txt");//��������� ����
		lockFile << "1";
		lockFile.close();

		arr<string> whereDel = splitString(".", dataFrom);// ���������� ������� ��� ��������
		string column;
		int numColumn;
		for (int i = 0; i < whereDel.currSize; i++) {// ���� ������������ ������ �������� ������ �������
			if (table == whereDel.pointer[i]) {
				column = whereDel.pointer[i + 1];
				numColumn = column[column.size() - 1] - 48;
				break;
			}
			else {
				continue;
			}
		}
		string stringFromFile, element;

		fstream file(currentPath, ios::in | ios::out | ios::binary);
		arr<string> buffer;//������� ������ � ������� ����� ���������� ��� ������� ������� �� �������� ��� ������
		if (file.is_open()) {
			while (getline(file, stringFromFile)) {
				if (file.eof()) {
					break;
				}
				element = splitString(",", stringFromFile).pointer[numColumn];

				if (element == filter) {

					continue;
				}

				if (element != filter) {
					buffer.push_back(stringFromFile);// �� ������� ���� ������� � \n !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				}

			}


		}

		file.close();
		if (!file.is_open()) { // ���������� �� �� ������ � ���� � ������ ������
			ofstream file(currentPath);
			file.seekp(0, ios::beg);
			for (int i = 0; i < buffer.currSize; i++) {
				file << buffer.pointer[i];
			}

			file.close();
		}

		ofstream unlockFile(nameSchema + '/' + table + '/' + table + "_lock.txt");// ������������ ����
		unlockFile << "0";
		unlockFile.close();
	}
	else if (tempString == "1"){// ���� ������������ �� �������
		
		cout << "File already is open. Try again letter." << endl;
	}
	//DELETE FROM table2 WHERE table2.column1 = '123'

}

void SelectWhere(string& fromTable, string SelectData, const arr<string>& paths, string whereRequest) {

	arr<string> tables = splitString(",", fromTable);
	arr<string> temp;
	arr<string> empty;
	arr<string> andOr;

	string modifiedString = "";
	temp = splitString(" ", whereRequest);
	for (int i = 0; i < temp.currSize; i++) {// �������� AND � OR �� ������ "*"
		if (temp.pointer[i] == "AND") {
			temp.pointer[i] = "*";
			andOr.push_back("AND");
		}
		if (temp.pointer[i] == "OR") {
			temp.pointer[i] = "*";
			andOr.push_back("OR");
		}
		modifiedString += temp.pointer[i];
	}
	// ��������� ���������
	arr<string> compares = splitString("*", modifiedString);// ��������� ��������� ������ �� *
	// ��������� �� ��������
	arr<string> indexToCompare, splitToCompare, columns;
	
	string column;
	int numColumn, isFirst;

	arr<string> arrOperands, splitedStr;
	string path, line;
	arr<string>  numbersString;
	arr<string> comparedStr, comparedIndex;

	comparedIndex.push_back(";");
	comparedStr.push_back(";");
	for (int i = 0; i < compares.currSize; i++) { // ��������� ��������� �� = 
		splitToCompare = splitString("=", compares.pointer[i]);

		for (int j = 0; j < splitToCompare.currSize; j++) {// ��������� ��������� �� . ��� ����������� table � column 

			columns = splitString(".", splitToCompare.pointer[j]);
			for (int k = 0; k < columns.currSize; k++) { // 2 ����� ��� ����������� �������� �������,  ��������� �� ��� � ��������� ����� ������ FROM

				for (int z = 0; z < tables.currSize; z++) {

					if (columns.pointer[k] == tables.pointer[z]) {// ���� �������� ������� ��������� �� 
						column = columns.pointer[k + 1];
						numColumn = column[column.size() - 1] - 48;// ������� ������ ������� � ������� �������

						path = tablenameToPath(columns.pointer[k], paths);// ���������� ���� �� �����

						ifstream file(path);

						isFirst = true;
						while (getline(file, line)) {// ������ ������ �����
							if (isFirst) {
								isFirst = false; //���� ������ ������ �� �������
								continue;
							}
							splitedStr = splitString(",", line); // ������� �����
							numbersString.push_back(splitedStr.pointer[0]); // ���������� �����

							arrOperands.push_back(splitedStr.pointer[numColumn]);// ���������� ������
						}
						isFirst = true;

						file.close();

					}
				
				}

			}

		}

		for (int j = 0; j < arrOperands.currSize / 2; j++) { // ��������� ������� = ��� ��� ��������� ��������� �������
			if (arrOperands.pointer[j] == arrOperands.pointer[j + arrOperands.currSize / 2]) {// ���� ��� ��������� �� ��������� ��� ���� � ������ � ����� ������ � ������ ������
				comparedStr.push_back(arrOperands.pointer[j] + "," + arrOperands.pointer[j + arrOperands.currSize / 2]);
				comparedIndex.push_back(numbersString.pointer[j]);

			}
		}

		comparedStr.push_back(";");// ��������� �������������� (�� ���� ����� �������� and ��� or)
		comparedIndex.push_back(";");
		arrOperands = empty;//������� �������
		numbersString = empty;
		
	}

	arr<string> numbersAfterAnd;
	int counter = 0, right, left;
	for (int i = 0; i < andOr.currSize; i++) {// �������� �� ���� ����������

		if (andOr.pointer[i] == "AND") {// ���� ��� AND
			for (int j = 1; j < comparedStr.currSize; j++) { // ���� ������ ; (�� ���� ����� �������� and)
				if (counter == i && comparedIndex.pointer[j] == ";") {// ���� ����� 

					left = j ;
					while (comparedStr.pointer[left - 1] != ";") {// ���� ������� ����� ������� ����������� � ����� ���������
						left--;
					}
					right = j;
					while (comparedStr.pointer[right + 1] != ";") {// ���� ������� ������ ������� ����������� � ����� ���������
						right++;
					}

					for (int k = left; k < j; k++) {
						for (int z = j + 1; z < right + 1; z++) {
							if (comparedIndex.pointer[k] == comparedIndex.pointer[z]) {// ���� ����� ������� ����� ������ ������ ������� �� ���������� ����� ������
								numbersAfterAnd.push_back(comparedIndex.pointer[k]);
							}
						}
					}

					
				}
				if (comparedStr.pointer[j] == ";") {
					counter++;
				}


			}

		}
		else {
			continue;
		}
		
		
	}
	bool isHere;
	int size;
	for (int i = 0; i < andOr.currSize; i++) {// �������� �� ���� ����������

		if (andOr.pointer[i] == "OR") {// ���� ��� AND
			for (int j = 1; j < comparedStr.currSize; j++) {// ���� ������ ; (�� ���� ����� �������� or)
				if (counter == i && comparedIndex.pointer[j] == ";") {// ���� �����

					left = j;
					while (comparedStr.pointer[left - 1] != ";") {// ���� ������� ����� ������� ����������� � ����� ���������
						left--;
					}
					right = j;
					while (comparedStr.pointer[right + 1] != ";") {// ���� ������� ������ ������� ����������� � ����� ���������
						right++;
					}

					for (int k = left; k < j; k++) {// ��������� ������ ����� �� ����� ����� ���� �� ��� � �������������� ������ 
						isHere = true;
						size = numbersAfterAnd.currSize;
						for (int z = 0; z < size; z++) {
							if (comparedIndex.pointer[k] == numbersAfterAnd.pointer[z]) {
								isHere = false;
							}
						}
						if (isHere) {
							numbersAfterAnd.push_back(comparedIndex.pointer[k]);
						}
					}
					for (int k = j + 1; k < right + 1; k++) {// ��������� ������ ����� �� ������ ����� ���� �� ��� � �������������� ������ 
						isHere = true;
						size = numbersAfterAnd.currSize;
						for (int z = 0; z < size; z++) {
							if (comparedIndex.pointer[k] == numbersAfterAnd.pointer[z]) {
								isHere = false;
							}
						}
						if (isHere) {
							numbersAfterAnd.push_back(comparedIndex.pointer[k]);
						}
					}

				}
				if (comparedStr.pointer[j] == ";") {
					counter++;
				}


			}

		}
		else {
			continue;
		}

	}

	string correctPath;

	arr<string> arrColumsToPrint = splitString(",", SelectData);
	arr<string> columsToPrint;
	arr<string> splittedLine;

	arr<string> indexResult;
	arr<string> result;
	for (int i = 0; i < arrColumsToPrint.currSize; i++) {
		columsToPrint = splitString(".", arrColumsToPrint.pointer[i]);
		for (int j = 0; j < columsToPrint.currSize; j++) {
			for (int k = 0; k < tables.currSize; k++) {
				if (columsToPrint.pointer[j] == tables.pointer[k]) {// �������� ������� ������� ������� �����
					correctPath = tablenameToPath(columsToPrint.pointer[j], paths);

					column = columsToPrint.pointer[j + 1];
					numColumn = column[column.size() - 1] - 48;// �������� �� �� ������ ������ �������
					

					ifstream file(correctPath);
					isFirst = true;
					while (getline(file, line)) {//������ ������ ������
						if (isFirst) {
							isFirst = false;// ���� ��� ������, �� ���������� �
							continue;
						}
						splittedLine = splitString(",", line); // ������� ������ �� "," � � ������� ������� �������� ������� � ����� �������
						for (int z = 0; z < numbersAfterAnd.currSize; z++) {
							if (numbersAfterAnd.pointer[z] == splittedLine.pointer[0]) {
								
								result.push_back(splittedLine.pointer[0] + "  " + splittedLine.pointer[numColumn]);
							}
						}
					}
					
					indexResult.push_back(toStr(result.currSize));// �������� ������ �����������
					result.push_back(",");// ��������� ��������� �������

				}
			}
		}
	}
	
	arr<string> leftArr, rightArr;

	for (int i = 0; i < toInt(indexResult.pointer[0]); i++) {//������ ����� ��� ����� �������
		for (int j = toInt(indexResult.pointer[0]) + 1; j < toInt(indexResult.pointer[1]); j++) {//�������� �� ������ �������
			leftArr = splitString(",", result.pointer[i]);
			rightArr = splitString(",", result.pointer[j]);
			cout << " \t" << leftArr.pointer[0] << "   " << leftArr.pointer[1] << " \t" << rightArr.pointer[0] << "   " << rightArr.pointer[1] << endl; // ������� �� ���������� ������������
		}
	}

	//SELECT table2.column1,table2.column2 FROM table1,table2 WHERE table1.column1 = table1.column2 AND table2.column1 = table2.column2
}