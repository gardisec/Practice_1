#include "Head.h"

template <typename T>
void arr<T> :: expand() {
    size_t newSize;
    newSize = maxSize + 10;

    T* newPointer = new T[newSize];
    for (size_t i = 0; i < currSize; ++i) {//������ ��������� �� ��������� �������
        newPointer[i] = pointer[i];
    }

    delete[] pointer;//������������ ��������� �� ������
    pointer = newPointer;
    maxSize = newSize;//��������� ������
}

template <typename T>
void arr<T> :: push_back(const T& value) {
    if (currSize >= maxSize) {//���� ������ ����� ����������� ������
        expand();
    }

    pointer[currSize++] = value;// �������� � �����
}

template <typename T>
void arr<T> :: remove(size_t position) {
    if (position >= currSize) {
        throw out_of_range("Out of range. Cannot remove");
    }

    for (size_t i = position; i < currSize - 1; ++i) {// �������� �������� ������ �� ����� ��������
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
