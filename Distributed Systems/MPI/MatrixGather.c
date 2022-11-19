#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

#define N 5
#define P 1000

int main() {
  int nProcs;
  int MyProcID;

  MPI_Init(NULL, NULL);
  MPI_Comm_size(MPI_COMM_WORLD, &nProcs);
  MPI_Comm_rank(MPI_COMM_WORLD, &MyProcID);
  if (nProcs != N * N) {
    printf("There must be %i processes\n", N * N);
  } else {
    MPI_Status status;
    double t0, t;
    int buf[N * N];
    int x = MyProcID % N;
    int y = MyProcID / N;
    int nR = (N - 1 - x);
    int i;
    t0 = MPI_Wtime();
    for (i = 0; i < P; i++) {
      buf[MyProcID] = MyProcID;
      if (x < N - 1)
        MPI_Recv(&buf[y * N + x + 1], nR, MPI_INT, MyProcID + 1, 0,
                 MPI_COMM_WORLD, &status);
      if (x > 0)
        MPI_Send(&buf[y * N + x], nR + 1, MPI_INT, MyProcID - 1, 0,
                 MPI_COMM_WORLD);
      if (x == 0) {
        int nU = (N - 1 - y) * N;
        if (y < N - 1)
          MPI_Recv(&buf[(y + 1) * N], nU, MPI_INT, MyProcID + N, 0,
                   MPI_COMM_WORLD, &status);
        if (y > 0)
          MPI_Send(&buf[y * N], nU + N, MPI_INT, MyProcID - N, 0,
                   MPI_COMM_WORLD);
      }
    }
    t = MPI_Wtime() - t0;
    if (x == 0 && y == 0) {
      int i;
      printf("Master [0,0] has received: [");
      for (i = 0; i < N * N; i++)
        printf("%i ", buf[i]);
      printf("]\n");
      printf("Average time per one GATHER = %lf seconds\n", t / P);
    }
  }
  MPI_Finalize();
  return 0;
}