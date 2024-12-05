#include "Head.h"

template <typename T>
void arr<T> :: expand() {
    size_t newSize;
    newSize = maxSize + 10;

    T* newPointer = new T[newSize];
    for (size_t i = 0; i < currSize; ++i) {//ставим указатель на последний элемент123
        newPointer[i] = pointer[i];
    }

    delete[] pointer;//переставляем указатель на массив
    pointer = newPointer;
    maxSize = newSize;//расширяем размер
}

template <typename T>
void arr<T> :: push_back(const T& value) {
    if (currSize >= maxSize) {//если массив полон увеличиваем размер
        expand();
    }

    pointer[currSize++] = value;// добавить в конец
}

template <typename T>
void arr<T> :: remove(size_t position) {
    if (position >= currSize) {
        throw out_of_range("Out of range. Cannot remove");
    }

    for (size_t i = position; i < currSize - 1; ++i) {// Сдвигаем элементы справа на место элемента
        pointer[i] = pointer[i + 1];
    }
    currSize--;
}

template <typename T>
void arr<T>::printArr() {
    for (int i = 0; i < currSize; ++i) {
        cout << pointer[i] << " ";
    }
    cout << endl;
}
