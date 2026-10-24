#define _GNU_SOURCE
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <omp.h>
static volatile uint64_t sink;
static double now(void){struct timespec t;clock_gettime(CLOCK_MONOTONIC_RAW,&t);return t.tv_sec+t.tv_nsec*1e-9;}
int main(void){
  size_t mb=getenv("BW_MB")?atol(getenv("BW_MB")):512, bytes=mb<<20, n=bytes/8;
  uint64_t *a=aligned_alloc(64,bytes),*b=aligned_alloc(64,bytes); if(!a||!b) abort();
  int P=omp_get_num_procs();
  #pragma omp parallel for
  for(size_t i=0;i<n;i++){a[i]=i*0x9E3779B97F4A7C15ULL;b[i]=0;}
  int th[3]={1,P/2>0?P/2:1,P};
  for(int t=0;t<3;t++){omp_set_num_threads(th[t]);
    for(int mode=0;mode<3;mode++){double best=1e9;
      for(int rep=0;rep<5;rep++){uint64_t s=0;double st=now();
        for(int pass=0;pass<4;pass++){
          if(mode==0){
            #pragma omp parallel for simd reduction(+:s)
            for(size_t i=0;i<n;i++) s+=a[i];}
          else if(mode==1){
            #pragma omp parallel for simd
            for(size_t i=0;i<n;i++) b[i]=a[i]+pass;}
          else{ /* widen-like: read 4B, write 8B */
            const int32_t *a32=(const int32_t*)a;
            #pragma omp parallel for simd
            for(size_t i=0;i<n;i++) b[i]=(int64_t)a32[i];}
        }
        double dt=now()-st; if(dt<best)best=dt; sink^=s^b[1];}
      double gb = mode==0? bytes*4./best/1e9 : mode==1? bytes*8./best/1e9 : (bytes/2.+bytes)*4./best/1e9;
      printf("threads=%d %s %.1f GB/s\n",th[t],mode==0?"read      ":mode==1?"read+write":"widen 4->8",gb);
      fflush(stdout);}}
  return 0;}
