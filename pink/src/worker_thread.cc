#include "worker_thread.h"

#include "pink/include/pink_conn.h"
#include "pink/src/pink_item.h"
#include "pink/src/pink_epoll.h"
#include "pink/src/csapp.h"

namespace pink {


WorkerThread::WorkerThread(ConnFactory *conn_factory, int cron_interval,
                           const ThreadEnvHandle* ehandle)
      : conn_factory_(conn_factory),
        cron_interval_(cron_interval),
        keepalive_timeout_(kDefaultKeepAliveTime) {
  set_env_handle(ehandle);
  /*
   * install the protobuf handler here
   */
  pthread_rwlock_init(&rwlock_, NULL);
  pink_epoll_ = new PinkEpoll();
  int fds[2];
  if (pipe(fds)) {
    exit(-1);
  }
  notify_receive_fd_ = fds[0];
  notify_send_fd_ = fds[1];
  pink_epoll_->PinkAddEvent(notify_receive_fd_, EPOLLIN | EPOLLERR | EPOLLHUP);
}

WorkerThread::~WorkerThread() {
  delete(pink_epoll_);
}

void *WorkerThread::ThreadMain() {
  int nfds;
  PinkFiredEvent *pfe = NULL;
  char bb[1];
  PinkItem ti;
  std::shared_ptr<PinkConn> in_conn;

  struct timeval when;
  gettimeofday(&when, NULL);
  struct timeval now = when;

  when.tv_sec += (cron_interval_ / 1000);
  when.tv_usec += ((cron_interval_ % 1000 ) * 1000);
  int timeout = cron_interval_;
  if (timeout <= 0) {
    timeout = PINK_CRON_INTERVAL;
  }

  while (!should_stop()) {

    if (cron_interval_ > 0) {
      gettimeofday(&now, NULL);
      if (when.tv_sec > now.tv_sec || (when.tv_sec == now.tv_sec && when.tv_usec > now.tv_usec)) {
        timeout = (when.tv_sec - now.tv_sec) * 1000 + (when.tv_usec - now.tv_usec) / 1000;
      } else {
        DoCronTask();
        when.tv_sec = now.tv_sec + (cron_interval_ / 1000);
        when.tv_usec = now.tv_usec + ((cron_interval_ % 1000 ) * 1000);
        timeout = cron_interval_;
      }
    }

    nfds = pink_epoll_->PinkPoll(timeout);

    for (int i = 0; i < nfds; i++) {
      pfe = (pink_epoll_->firedevent()) + i;
      if (pfe->fd == notify_receive_fd_) {
        if (pfe->mask & EPOLLIN) {
          //read(notify_receive_fd_, bb, 1);
          //{
          //  slash::MutexLock l(&mutex_);
          //  ti = conn_queue_.front();
          //  conn_queue_.pop();
          //}
          //PinkConn *tc = conn_factory_->NewPinkConn(ti.fd(), ti.ip_port(), this);
          //tc->SetNonblock();
          //{
          //  slash::RWLock l(&rwlock_, true);
          //  conns_[ti.fd()] = tc;
          //}
          //pink_epoll_->PinkAddEvent(ti.fd(), EPOLLIN);
          
          char tmp[2048];
          int32_t nread = read(notify_receive_fd_, tmp, 2048);
          if (nread <= 0) {
            continue;
          }
          for (int32_t idx = 0; idx != nread; ++idx) {
            {
              slash::MutexLock l(&mutex_);
              ti = conn_queue_.front();
              conn_queue_.pop();
            }
            PinkConn *tc = conn_factory_->NewPinkConn(ti.fd(), ti.ip_port(), this);
            tc->SetNonblock();
            {
              slash::RWLock l(&rwlock_, true);
              conns_[ti.fd()].reset(tc);
            }
            pink_epoll_->PinkAddEvent(ti.fd(), EPOLLIN);
          }
        } else {
          continue;
        }
      } else {
        in_conn.reset();
        int should_close = 0;
        if (pfe == NULL) {
          continue;
        }
        pthread_rwlock_rdlock(&rwlock_);
        std::map<int, std::shared_ptr<PinkConn> >::iterator iter = conns_.find(pfe->fd);
        if (iter == conns_.end()) {
          pthread_rwlock_unlock(&rwlock_);
          pink_epoll_->PinkDelEvent(pfe->fd);
          continue;
        }
        pthread_rwlock_unlock(&rwlock_);


        in_conn = iter->second;
        ReadStatus getRes;
        if (pfe->mask & EPOLLIN) {
          getRes = in_conn->GetRequest();
          in_conn->set_last_interaction(now);
          if (getRes != kReadAll && getRes != kReadHalf) {
            // kReadError kReadClose kFullError kParseError
            should_close = 1;
          } else if (in_conn->is_reply()) {
            pink_epoll_->PinkModEvent(pfe->fd, EPOLLIN, EPOLLOUT);
          } else {
            continue;
          }
        }
        if (!should_close && pfe->mask & EPOLLOUT) {
          WriteStatus write_status = in_conn->SendReply();
          if (write_status == kWriteAll) {
            in_conn->set_is_reply(false);
            pink_epoll_->PinkModEvent(pfe->fd, 0, EPOLLIN);
          } else if (write_status == kWriteHalf) {
            continue;
          } else if (write_status == kWriteError) {
            should_close = 1;
          }
        }
        if ((pfe->mask & EPOLLERR) || (pfe->mask & EPOLLHUP) || should_close) {
          std::string info;
          if (pfe->mask & EPOLLERR) {
            info.append("EPOLLERR ");
          }
          if (pfe->mask & EPOLLHUP) {
            info.append("EPOLLHUP ");
          }
          if (should_close) {
            if (pfe->mask & EPOLLIN) {
              if (getRes == kReadClose) {
                info.append("In close");
              } else {
                info.append("In error");
              }
            } else {
              info.append("Out error");
            } 
          }
          in_conn->Cleanup(info);
          {
            slash::RWLock l(&rwlock_, true);
            conns_.erase(iter);
          }
          pink_epoll_->PinkDelEvent(pfe->fd);
          close(pfe->fd);
          in_conn.reset();
        }
      } // connection event
    } // for (int i = 0; i < nfds; i++)
		if (ehandle()) {
			ehandle()->Sanitize(get_private());
		}
  } // while (!should_stop())

  Cleanup();
  return NULL;
}

void WorkerThread::DoCronTask() {
  if (keepalive_timeout_ <= 0) {
    return;
  }
  // Check keepalive timeout connection
  struct timeval now;
  gettimeofday(&now, NULL);
  std::vector<std::shared_ptr<PinkConn> > to_cleanup;

  int32_t conn_fd = 0;
  {
    slash::RWLock l(&rwlock_, true);
    std::map<int, std::shared_ptr<PinkConn> >::iterator iter = conns_.begin();
    while (iter != conns_.end()) {
      int32_t r = iter->second->DoCron(now);
      if (r == -1) {
        to_cleanup.push_back(iter->second);
        conn_fd = iter->first;
        iter = conns_.erase(iter);
        pink_epoll_->PinkDelEvent(conn_fd);
        close(conn_fd);
        continue;
      } else if (r == 1) {
        pink_epoll_->PinkModEvent(iter->second->fd(), EPOLLIN, EPOLLOUT);
      }
      ++iter;
    }
  }
  for (std::shared_ptr<PinkConn>& item : to_cleanup) {
    item->Cleanup("Heartbeat timeout");
  }
}

void WorkerThread::Cleanup() {
  std::map<int, std::shared_ptr<PinkConn> >::iterator iter;
  std::vector<std::shared_ptr<PinkConn> > to_cleanup;
  {
    slash::RWLock l(&rwlock_, true);
    for (iter = conns_.begin(); iter != conns_.end();) {
      close(iter->first);
      to_cleanup.push_back(iter->second);
      iter = conns_.erase(iter);
    }
  }
  for (std::shared_ptr<PinkConn>& item : to_cleanup) {
    item->Cleanup();
  }
}

};  // namespace pink
