#pragma once

#include <cstdlib>
#include <sys/time.h>

class timer
{
public:
  timer()
  {
    total.tv_sec = total.tv_usec = 0;
    diff.tv_sec = diff.tv_usec = 0;
  }

  double duration()
  {
    double duration;

    duration = (total.tv_sec) * 1000000.0; // sec to us
    duration += (total.tv_usec);           // us

    return duration * 1000.0; //ns
  }

  void start()
  {
    gettimeofday(&t1, NULL);
  }

  void end()
  {
    gettimeofday(&t2, NULL);
    timersub(&t2, &t1, &diff);
    timeradd(&diff, &total, &total);
  }

  void reset()
  {
    total.tv_sec = total.tv_usec = 0;
    diff.tv_sec = diff.tv_usec = 0;
  }

  timeval t1, t2, diff;
  timeval total;
};

class nsTimer
{
public:
  struct timespec t1, t2;
  long long diff, total, count, abnormal, normal;

  nsTimer()
  {
    reset();
  }
  void start()
  {
    clock_gettime(CLOCK_MONOTONIC, &t1);
  }
  long long end(bool flag = false)
  {
    clock_gettime(CLOCK_MONOTONIC, &t2);
    diff = (t2.tv_sec - t1.tv_sec) * 1000000000 +
           (t2.tv_nsec - t1.tv_nsec);
    total += diff;
    count++;
    if (diff > 10000000)
      abnormal++;
    if (diff < 10000)
      normal++;
    return diff;
  }
  long long op_count()
  {
    return count;
  }
  void reset()
  {
    diff = total = count = 0;
  }
  long long duration()
  { // ns
    return total;
  }
  double avg()
  { // ns
    return double(total) / count;
  }
  double abnormal_rate()
  {
    return double(abnormal) / count;
  }
  double normal_rate()
  {
    return double(normal) / count;
  }
};
