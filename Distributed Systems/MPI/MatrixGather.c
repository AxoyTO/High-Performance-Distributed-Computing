#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

#define N 5  // Размерность транспьютерной матрицы
#define P 10000  // Число итераций(повторов) передач типа MPI_Gather

int main() {
  int nProcs;    // Общее число процессов
  int MyProcID;  // Идентификатор процесса

  MPI_Init(NULL, NULL);                    // Инициализация MPI
  MPI_Comm_size(MPI_COMM_WORLD, &nProcs);  // Получение числа процессов
  MPI_Comm_rank(MPI_COMM_WORLD,
                &MyProcID);  // Получение идентификатора процесса

  // Проверка числа процессов, если число процессов не равно N*N,
  // то выводится сообщение об ошибке и завершается работа программы
  if (nProcs != N * N) {
    printf("There must be %i processes\n", N * N);
  } else {
    MPI_Status status;  // Статус операции MPI
    double t0, t;    // Время начала и конца передачи
    int buf[N * N];  // Буфер для передачи, собирание данных в него
    int x = MyProcID % N;  // Координата x процесса в транспьютерной матрице
    int y = MyProcID / N;  // Координата y процесса в транспьютерной матрице
    // printf("Process [%i,%i] has started with id %i\n", x, y, MyProcID);
    int nR = (N - 1 - x);  // Число принимаемых справа элементов данных
    int i;
    t0 = MPI_Wtime();  // Засекаем время начала передачи
    for (i = 0; i < P; i++) {    // Повторяем передачу P раз
      buf[MyProcID] = MyProcID;  // Процесс помещает в буфер свой идентификатор

      // Если процесс не находится на правой границе транспьютерной матрицы N*N
      if (x < N - 1) {
        // Процесс принимает от процесса справа nR элементов данных
        MPI_Recv(&buf[y * N + x + 1], nR, MPI_INT, MyProcID + 1, 0,
                 MPI_COMM_WORLD, &status);  // Процесс принимает от процесса
                                            // справа nR элементов данных
        if (P == 1) {
          printf(
              "Process [%i,%i] has received %i element(s) from process "
              "[%i,%i]. "
              "The element(s): ",
              x, y, nR, x + 1, y);
          for (int j = 0; j < nR; j++)
            printf("%i ", buf[y * N + x + 1 + j]);
          printf("\n");
        }
      }
      // Если процесс не находится на левой границе транспьютерной матрицы N*N
      if (x > 0) {
        MPI_Send(&buf[y * N + x], nR + 1, MPI_INT, MyProcID - 1, 0,
                 MPI_COMM_WORLD);  // Процесс отправляет процессу слева nR+1
                                   // элементов данных
      }
      // Если процесс находится на левой границе транспьютерной матрицы N*N
      if (x == 0) {
        int nU = (N - 1 - y) * N;  // Число принимаемых снизу элементов данных
        // Если процесс не находится на нижней границе транспьютерной матрицы
        if (y < N - 1) {
          MPI_Recv(&buf[(y + 1) * N], nU, MPI_INT, MyProcID + N, 0,
                   MPI_COMM_WORLD, &status);  // Процесс принимает от процесса
          // сверху nU элементов данных
          if (P == 1) {
            printf(
                "Process [%i,%i] has received %i element(s) from process "
                "[%i,%i]. "
                "The element(s): ",
                x, y, nU, x, y + 1);
            for (int j = 0; j < nU; j++)
              printf("%i ", buf[(y + 1) * N + j]);
            printf("\n");
          }
        }
        // Если процесс не находится на нижней границе транспьютерной матрицы
        if (y > 0)
          MPI_Send(&buf[y * N], nU + N, MPI_INT, MyProcID - N, 0,
                   MPI_COMM_WORLD);  // Процесс отправляет процессу снизу nU+N
                                     // элементов данных
      }
    }
    t = MPI_Wtime() - t0;  // Время выполнения операции GATHER
    if (x == 0 && y == 0) {  // Если процесс находится в левом верхнем углу
                             // транспьютерной матрицы N*N
      int i;
      printf("Master [0,0] has received: [");
      for (i = 0; i < N * N; i++)
        printf("%i ", buf[i]);
      printf("]\n");
      printf("Average time per one GATHER = %lf seconds\n", t / P);
    }
    MPI_Finalize();  // Завершение работы MPI
    return 0;
  }
}