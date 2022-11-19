#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
// Размерность транспьютерной матрицы
#define N 5
// Число повторов передач типа MPI_Gather
#define P 1000

int main() {
	int nProcs; // Общее число процессов
	int MyProcID; // Идентификатор текущего процечса

	MPI_Init(NULL, NULL); // Инициализируем MPI
	// Получаем информацию о параллельной системе
	MPI_Comm_size(MPI_COMM_WORLD, &nProcs);
	MPI_Comm_rank(MPI_COMM_WORLD, &MyProcID);
	if (nProcs != N*N) { // Если число запущенных процессов не совпадает с размерностью матрицы
		printf("Must be %i processes\n", N*N);
	} else {
		MPI_Status status; // Статус операции приема
		double t0, t; // Время
		int buf[N*N]; // Собираемые данные
		// Получаем координаты процесса в матрице
		int x = MyProcID % N;
		int y = MyProcID / N;
		int nR = (N-1-x); // Число принимаемых справа элементов данных
		int i;
		t0 = MPI_Wtime(); // Временная засечка
		for (i=0; i < P; i++) { // Цикл повтора пересылок
			// Пусть процессы пересылают свой идентификатор
			buf[MyProcID] = MyProcID;
			if (x < N-1)
				MPI_Recv(&buf[y*N+x+1], nR, MPI_INT, MyProcID+1, 1234, MPI_COMM_WORLD, &status);
			if (x > 0)
				MPI_Send(&buf[y*N+x], nR+1, MPI_INT, MyProcID-1, 1234, MPI_COMM_WORLD);
			// Левый край матрицы
			if (x == 0) {
				int nU = (N-1-y)*N; // Число принимаемых сверху элементов данных
				if (y < N-1)
					MPI_Recv(&buf[(y+1)*N], nU, MPI_INT, MyProcID+N, 1234, MPI_COMM_WORLD, &status);
				if (y > 0)
					MPI_Send(&buf[y*N], nU+N, MPI_INT, MyProcID-N, 1234, MPI_COMM_WORLD);
			}
		}
		t = MPI_Wtime() - t0; // Получаем время
		if (x == 0 && y == 0) {
			int i;
			printf("Master [0,0] has received: [");
			for (i = 0; i < N*N; i++)
				printf("%i ", buf[i]);
			printf("]\n");
			printf("Average time per one GATHER = %lf seconds\n", t/P);
		}
	}

	MPI_Finalize(); // Деинициализация MPI

	return 0;
}