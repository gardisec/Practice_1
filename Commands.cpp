#include "Head.h"

void Insert(const string& table, arr<string>& data, const arr<string>& paths) {

	
	const int tuple_limit = toInt(readSchema("schema.json").pointer[2]);// Считываем данные с файла json 123131
	string correctPath = tablenameToPath(table, paths);
	string nameSchema = readSchema("schema.json").pointer[0];

	string tempString, pk;
	ifstream lockFile(nameSchema + '/' + table + '/' + table + "_lock.txt");//проверка на блокировку файла
	getline(lockFile, tempString);
	lockFile.close();

	if (tempString == "0") {// если разблокирован файл, то начинаем работу с ним
		
		ofstream lockFile(nameSchema + '/' + table + '/' + table + "_lock.txt");// блокировка файла
		lockFile << "1";
		lockFile.close();
		string line;
		ifstream file(correctPath);//читаем первую строчку
		if (file.is_open()) {
			getline(file, line);
			file.close();
		}
		arr<string> maxColums = splitString(",", line);//сплитим строчкуу по , и можем обратиться к её размеру

		if (maxColums.currSize - 1 == data.currSize) {// если кол-во данных в запросе равняется максимальному кол-ву данных в строке файла
			
			ifstream pkCount(nameSchema + '/' + table + '/' + table + "_pk_sequence.txt");//Находим номер строчки
			getline(pkCount, pk);
			pkCount.close();

			
			ofstream file(correctPath, ios::ate | ios::app);//записываем номер строчки и данные
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
		
		ofstream unlockFile(nameSchema + '/' + table + '/' + table + "_lock.txt");// разблокирование файла
		unlockFile << "0";
		unlockFile.close();

		ofstream pkFileRecord(nameSchema + '/' + table + '/' + table + "_pk_sequence.txt");//инкрементируем pk и записываем в файл
		pkFileRecord << toInt(pk) + 1;
		pkFileRecord.close();


	}
	else if (tempString == "1") {//если над файлом уже работают, то выходим
		
		cerr << "File already is open. Try again letter." << endl;
	}
	
}//INSERT INTO table2 VALUES ('zxczxb', 'asdasdg')

void Select(string& fromTable, string SelectData, const arr<string>& paths) {

	arr<string> allChose = splitString(",", SelectData);// заспличен запрос 
	arr<string> temp;
	arr<string> empty;
	arr<string> splitedChose;

	for (int i = 0; i < allChose.currSize; i++) {// на выходе получаем массив таблица , колонка ; след запрос тааблица , колонка 
		temp = splitString(".", allChose.pointer[i]);
		for (int j = 0; j < temp.currSize; j++) {
			splitedChose.push_back(temp.pointer[j]);
		}

	}
	temp = empty;//очистка массива
	int numColumn, count;
	string currentPath, column, stringFromFile;
	arr<string> tables = splitString(",", fromTable);// массив с названиями таблиц в которых работаем
	for (int i = 0; i < tables.currSize; i++) {

		currentPath = tablenameToPath(tables.pointer[i], paths);//определяем путь до .csv файла по азванию таблицы 


		for (int j = 0; j < splitedChose.currSize; j++) {
			count = 0;
			if (tables.pointer[i] == splitedChose.pointer[j]) {// если название таблицы совпадает, то следующий элемент в массиве это колонка к которой обращаемся
				column = splitedChose.pointer[j + 1];
				numColumn = column[column.size() - 1] - 48;// находим номер колонки к которой обращаемся

			}
			else {
				continue;
			}

			ifstream file(currentPath);

			if (file.is_open()) {
				while (getline(file, stringFromFile)) {// считываем строчку
					if (file.eof()) {// если конец файла то выходим
						file.close();
						break;
					}
					if (count == 0) {//если строчка первая в файле то пропускаем
						count++;
						continue;
					}
					else {//добавляем в массив номер строчки и элемент строчки находящийся в нужной колонке через запятую
						temp.push_back(splitString(",", stringFromFile).pointer[0] + "," + splitString(",", stringFromFile).pointer[numColumn]);
					}

				}
				temp.push_back(",");// ставим запятую, что ограничивает данные взятые по 1 запросу
			}


		}
	}

	arr<string> indexTemp;
	for (int i = 0; i < temp.currSize; i++) {// заполняем массив с индексами окончания данных одного запроса
		if (temp.pointer[i] == ",") {
			indexTemp.push_back(toStr(i));
		}
	}
	arr<string> left, right;

	for (int i = 0; i < toInt(indexTemp.pointer[0]); i++) {//делаем вывод для двух колонок
		for (int j = toInt(indexTemp.pointer[0]) + 1; j < toInt(indexTemp.pointer[1]); j++) {//проходит по первой колонке
			left = splitString(",", temp.pointer[i]);
			right = splitString(",", temp.pointer[j]);
			cout << " \t" << left.pointer[0] << "   " << left.pointer[1] << " \t" << right.pointer[0] << "   " << right.pointer[1] << endl;
		}
	}

}
//SELECT table2.column1,table2.column2 FROM table2

void Delete(const string& table,const string& dataFrom,const string& filter,const arr<string>& path) {
	string currentPath = tablenameToPath(table, path);// определяем путь до файла над которым работаем

	string nameSchema = readSchema("schema.json").pointer[0];// определяем имя схемы
	string tempString;

	ifstream lockFile(nameSchema + '/' + table + '/' + table + "_lock.txt");// получаем ключ блокировки
	getline(lockFile, tempString);
	lockFile.close();

	if (tempString == "0") {// если разблокировано, то работаем

		ofstream lockFile(nameSchema + '/' + table + '/' + table + "_lock.txt");//блокируем файл
		lockFile << "1";
		lockFile.close();

		arr<string> whereDel = splitString(".", dataFrom);// определяем колонку для проверки
		string column;
		int numColumn;
		for (int i = 0; i < whereDel.currSize; i++) {// цикл определяющий индекс элемента нужной колонки
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
		arr<string> buffer;//создаем буффер в который будем записывать все строчки которые не подходят под фильтр
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
					buffer.push_back(stringFromFile);// на линуксе надо сделать с \n !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				}

			}


		}

		file.close();
		if (!file.is_open()) { // записываем всё из буфера в файл с самого начала
			ofstream file(currentPath);
			file.seekp(0, ios::beg);
			for (int i = 0; i < buffer.currSize; i++) {
				file << buffer.pointer[i];
			}

			file.close();
		}

		ofstream unlockFile(nameSchema + '/' + table + '/' + table + "_lock.txt");// разблокируем файл
		unlockFile << "0";
		unlockFile.close();
	}
	else if (tempString == "1"){// если заблокирован то выходим
		
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
	for (int i = 0; i < temp.currSize; i++) {// заменяем AND и OR на символ "*"
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
	// обработка сравнения
	arr<string> compares = splitString("*", modifiedString);// разбиваем заменённую строку по *
	// разбиваем на операнды
	arr<string> indexToCompare, splitToCompare, columns;
	
	string column;
	int numColumn, isFirst;

	arr<string> arrOperands, splitedStr;
	string path, line;
	arr<string>  numbersString;
	arr<string> comparedStr, comparedIndex;

	string filtr;
	comparedIndex.push_back(";");
	comparedStr.push_back(";");
	for (int i = 0; i < compares.currSize; i++) { // разбиваем подстроки по = 
		splitToCompare = splitString("=", compares.pointer[i]);

		for (int j = 0; j < splitToCompare.currSize; j++) {// разбиваем подстроки по . для определения table и column  

			columns = splitString(".", splitToCompare.pointer[j]);
			for (int k = 0; k < columns.currSize; k++) { // 2 цикла для определения названия таблицы,  совпадает ли оно с заданными после токена FROM

				for (int z = 0; z < tables.currSize; z++) {

					if (columns.pointer[k] == tables.pointer[z]) {// если название таблицы совпадает то  
						column = columns.pointer[k + 1];
						numColumn = column[column.size() - 1] - 48;// считаем индекс колонки с нужными данными

						path = tablenameToPath(columns.pointer[k], paths);// определяем путь до файла

						ifstream file(path);

						isFirst = true;
						while (getline(file, line)) {// читаем строки файла
							if (isFirst) {
								isFirst = false; //если строка первая то выходим
								continue;
							}
							splitedStr = splitString(",", line); // сплитим линию
							numbersString.push_back(splitedStr.pointer[0]); // записываем номер

							arrOperands.push_back(splitedStr.pointer[numColumn]);// записываем данные
						}
						isFirst = true;

						file.close();

					}
				
				}

			}

		}

				for (int j = 0; j < arrOperands.currSize / 2; j++) { // проверяем условие = для пар элементов выбранных колонок
			if (arrOperands.pointer[j] == arrOperands.pointer[j + arrOperands.currSize / 2]) {// если они совпадают то добавляем эту пару в массив и номер строки в другой массив
				comparedStr.push_back(arrOperands.pointer[j] + "," + arrOperands.pointer[j + arrOperands.currSize / 2]);
				comparedIndex.push_back(numbersString.pointer[j]);

			}
		}

		comparedStr.push_back(";");// добавляем разграничитель (на этом месте оператор and или or)
		comparedIndex.push_back(";");
		arrOperands = empty;//очищаем массивы
		numbersString = empty;
		
	}

	arr<string> numbersAfterAnd;
	int counter = 0, right, left;
	for (int i = 0; i < andOr.currSize; i++) {// проходим по всем операторам

		if (andOr.pointer[i] == "AND") {// если это AND
			for (int j = 1; j < comparedStr.currSize; j++) { // ищем символ ; (на этом месте оператор and)
				if (counter == i && comparedIndex.pointer[j] == ";") {// если нашли 

					left = j ;
					while (comparedStr.pointer[left - 1] != ";") {// ищем крайний левый элемент относящийся к этому оператору
						left--;
					}
					right = j;
					while (comparedStr.pointer[right + 1] != ";") {// ищем крайний правый элемент относящийся к этому оператору
						right++;
					}

					for (int k = left; k < j; k++) {
						for (int z = j + 1; z < right + 1; z++) {
							if (comparedIndex.pointer[k] == comparedIndex.pointer[z]) {// если левый элемент равен какому нибудь правому то записываем номер строки
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
	for (int i = 0; i < andOr.currSize; i++) {// проходим по всем операторам

		if (andOr.pointer[i] == "OR") {// если это AND
			for (int j = 1; j < comparedStr.currSize; j++) {// ищем символ ; (на этом месте оператор or)
				if (counter == i && comparedIndex.pointer[j] == ";") {// если нашли

					left = j;
					while (comparedStr.pointer[left - 1] != ";") {// ищем крайний левый элемент относящийся к этому оператору
						left--;
					}
					right = j;
					while (comparedStr.pointer[right + 1] != ";") {// ищем крайний правый элемент относящийся к этому оператору
						right++;
					}

					for (int k = left; k < j; k++) {// добавляем номера строк из левой части если их нет в результирующем списке 
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
					for (int k = j + 1; k < right + 1; k++) {// добавляем номера строк из правой части если их нет в результирующем списке 
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
				if (columsToPrint.pointer[j] == tables.pointer[k]) {// получаем колонки которые вывести нужно
					correctPath = tablenameToPath(columsToPrint.pointer[j], paths);

					column = columsToPrint.pointer[j + 1];
					numColumn = column[column.size() - 1] - 48;// получаем из их номера индекс колонки
					

					ifstream file(correctPath);
					isFirst = true;
					while (getline(file, line)) {//читаем каждую строку
						if (isFirst) {
							isFirst = false;// если она первая, то пропускаем её
							continue;
						}
						splittedLine = splitString(",", line); // сплитим строку по "," и с помощью индекса получаем элемент и номер строчки
						for (int z = 0; z < numbersAfterAnd.currSize; z++) {
							if (numbersAfterAnd.pointer[z] == splittedLine.pointer[0]) {
								
								result.push_back(splittedLine.pointer[0] + "  " + splittedLine.pointer[numColumn]);
							}
						}
					}
					
					indexResult.push_back(toStr(result.currSize));// получаем индекс разделителя
					result.push_back(",");// разделяем найденные колонки

				}
			}
		}
	}
	
	arr<string> leftArr, rightArr;

	for (int i = 0; i < toInt(indexResult.pointer[0]); i++) {//делаем вывод для левой колонки
		for (int j = toInt(indexResult.pointer[0]) + 1; j < toInt(indexResult.pointer[1]); j++) {//проходит по первой колонке
			leftArr = splitString(",", result.pointer[i]);
			rightArr = splitString(",", result.pointer[j]);
			cout << " \t" << leftArr.pointer[0] << "   " << leftArr.pointer[1] << " \t" << rightArr.pointer[0] << "   " << rightArr.pointer[1] << endl; // выводим их декартовое произведение
		}
	}

	//SELECT table2.column1,table2.column2 FROM table1,table2 WHERE table1.column1 = table1.column2 AND table2.column1 = table2.column2
}