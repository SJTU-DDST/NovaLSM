#include <pthread.h>
#include <map>
#include <mutex>
#include <fmt/core.h>
#include<functional>
#include "qp.hpp"
#include "common.hpp"

namespace rdmaio {

/**
 * Simple critical section
 * It uses a single global block to guard RdmaCtrl.
 * This is acceptable, since RdmaCtrl is only the control plane.
 */

//全局锁
    class SCS {
    public:
        SCS() {
            get_lock().lock();
        }

        ~SCS() {
            get_lock().unlock();
        }

    private:
        static std::mutex &get_lock() {
            static std::mutex lock;
            return lock;
        }
    };

/**
 * convert qp idx(node,worker,idx) -> key
 */
    inline uint32_t get_rc_key(const QPIdx idx) {
        // 2^10 (1024) conn_workers. 2^6 (64) nodes. 2^16 (65536) qps per worker
        return static_cast<uint32_t>(static_cast<uint32_t>(idx.node_id) << 26) | // node
               (static_cast<uint32_t>(idx.worker_id) << 16) | // worker
               static_cast<uint32_t>(idx.index); // qp
    }

    inline QPIdx get_rc_idx(const uint32_t rc_key) {
        int node_id = rc_key >> 26;
        int worker_id = (rc_key - (node_id << 26)) >> 16;
        int index = rc_key - (node_id << 26) - (worker_id >> 16);
        return QPIdx{node_id, worker_id, index};
    }

    inline uint32_t get_ud_key(const QPIdx idx) {
        return
                (static_cast<uint32_t>(idx.worker_id) << 16) |
                static_cast<uint32_t>(idx.index);
    }

/**
 * Control plane of RLib
 */
    class RdmaCtrl::RdmaCtrlImpl {
    public:
        RdmaCtrlImpl(int node_id, int tcp_base_port,
                     connection_callback_t callback, std::string local_ip) :
                node_id_(node_id),
                tcp_base_port_(tcp_base_port),
                local_ip_(local_ip),
                qp_callback_(callback) {
            // start the background thread to handle QP connection request
//启动线程处理qp类型的连接请求
            pthread_attr_t attr;
            pthread_attr_init(&attr);
            pthread_create(&handler_tid_, &attr,
                           &RdmaCtrlImpl::connection_handler_wrapper, this);
        }

        ~RdmaCtrlImpl() {
            running_ = false; // wait for the handler to join
            pthread_join(handler_tid_, NULL);
            NOVA_LOG(INFO)
                << "rdma controler close: does not handle any future connections.";
        }

//打开
        RNicHandler *open_thread_local_device(DevIdx idx) {
            // already openend device
            if (rnic_instance() != nullptr)
                return rnic_instance();

            auto handler = open_device(idx);
            rnic_instance() = handler;
            return rnic_instance();
        }

// rdmahandler->broker->rdmactrl 这里外面上锁了 其实和单例差不多?? 分配了pd!
//打开idx指定的设备，分配一些东西，然后返回
        RNicHandler *open_device(DevIdx idx) {

            RNicHandler *rnic = nullptr;

            struct ibv_device **dev_list = nullptr;
            struct ibv_context *ib_ctx = nullptr;
            struct ibv_pd *pd = nullptr;
            int num_devices;
            int rc;  // return code
            ibv_port_attr port_attr = {};

            dev_list = ibv_get_device_list(&num_devices);

            if (idx.dev_id >= num_devices || idx.dev_id < 0) {
                NOVA_LOG(WARNING) << "wrong dev_id: " << idx.dev_id
                                  << "; total " << num_devices << " found";
                goto OPEN_END;
            }

            // alloc ctx
            ib_ctx = ibv_open_device(dev_list[idx.dev_id]);
            if (ib_ctx == nullptr) {
                NOVA_LOG(WARNING) << "failed to open ib ctx w error: "
                                  << strerror(errno);
                goto OPEN_END;
            }

            // alloc pd
            pd = ibv_alloc_pd(ib_ctx);
            if (pd == nullptr) {
                NOVA_LOG(WARNING) << "failed to alloc pd w error: "
                                  << strerror(errno);
                RDMA_VERIFY(INFO, ibv_close_device(ib_ctx) == 0)
                    << "failed to close device " << idx.dev_id;
                goto OPEN_END;
            }

            // fill the lid

            rc = ibv_query_port (ib_ctx, idx.port_id, &port_attr);
            if (rc < 0) {
                NOVA_LOG(WARNING) << "failed to query port status w error: "
                                  << strerror(errno);
                RDMA_VERIFY(INFO, ibv_close_device(ib_ctx) == 0)
                    << "failed to close device " << idx.dev_id;
                RDMA_VERIFY(INFO, ibv_dealloc_pd(pd) == 0)
                    << "failed to dealloc pd";
                goto OPEN_END;
            }

            NOVA_LOG(INFO) << "mtu " << port_attr.active_mtu << ":"
                           << port_attr.max_mtu << ":" << port_attr.lid;

            // success open
            {
                rnic = new RNicHandler(idx.dev_id, idx.port_id, ib_ctx, pd,
                                       port_attr.lid);
            }

            OPEN_END:
            if (dev_list != nullptr)
                ibv_free_device_list(dev_list);
            return rnic;
        }

        RCQP *get_rc_qp(QPIdx idx) {
            RCQP *res = nullptr;
            {
                SCS s;
                res = get_qp<RCQP, get_rc_key>(idx);
            };
            return res;
        }

        UDQP *get_ud_qp(QPIdx idx) {

            UDQP *res = nullptr;
            {
                SCS s;
                res = get_qp<UDQP, get_ud_key>(idx);
            };
            return res;
        }

        /**
         * Note! this is not a thread-safe function
         */
        template<class T, uint32_t (*F)(QPIdx)>
        T *get_qp(QPIdx idx) {
            uint32_t key = F(idx);
            if (qps_.find(key) == qps_.end())
                return nullptr;
            else
                return dynamic_cast<T *>(qps_[key]);
        }
//rdma_handler::start->nova_rdmarc_broker::init->nova_rdmarc_broker::InitializeQPs
// 自己的idx , ~, 之前登记的第一个mr的attr, ~, 发送cq和接收cq
//建立发送和接收的qp
        RCQP *create_rc_qp(QPIdx idx, RNicHandler *dev, MemoryAttr *attr,
                           enum ibv_qp_type qp_type, ibv_cq *cq,
                           ibv_cq *recv_cq) {

            RCQP *res = nullptr;
            {
                SCS s;
                uint64_t qid = get_rc_key(idx);
//找到了的话不用create
                if (qps_.find(qid) != qps_.end()) {
                    res = dynamic_cast<RCQP *>(qps_[qid]);
//没找到的话，建立这个结构并且加进去                
                } else {
                    if (attr == NULL)
                        res = new RCQP(dev, idx, qp_type, cq, recv_cq);
                    else
                        res = new RCQP(dev, idx, *attr, qp_type, cq, recv_cq);
                    qps_.insert(std::make_pair(qid, res));
                }
            };
            return res;
        }

        RCQP *destroy_rc_qp(QPIdx idx) {
            SCS s;
            uint64_t qid = get_rc_key(idx);
            NOVA_ASSERT(qps_.find(qid) != qps_.end());
            RCQP *res = dynamic_cast<RCQP *>(qps_[qid]);

            NOVA_ASSERT(ibv_destroy_qp(res->qp_) == 0)
                << fmt::format("destroy qp {}-{}-{} failed {}", idx.node_id,
                               idx.worker_id, idx.index, strerror(errno));

            NOVA_ASSERT(ibv_destroy_cq(res->cq_) == 0)
                << fmt::format("destroy scq qp {}-{}-{} failed {}",
                               idx.node_id,
                               idx.worker_id, idx.index, strerror(errno));
            NOVA_ASSERT(ibv_destroy_cq(res->recv_cq_) == 0)
                << fmt::format("destroy rcq qp {}-{}-{} failed {}",
                               idx.node_id,
                               idx.worker_id, idx.index, strerror(errno));
            delete res;
            qps_.erase(qid);
        }

        UDQP *create_ud_qp(QPIdx idx, RNicHandler *dev, MemoryAttr *attr) {

            UDQP *res = nullptr;
            uint64_t qid = get_ud_key(idx);

            {
                SCS s;
                if (qps_.find(qid) != qps_.end()) {
                    res = dynamic_cast<UDQP *>(qps_[qid]);
                } else {
                    if (attr == NULL)
                        res = new UDQP(dev, idx);
                    else
                        res = new UDQP(dev, idx, *attr);
                    qps_.insert(std::make_pair(qid, res));
                }
            };
            return res;
        }

// rdma_handler::start->nova_rdmarc_broker::init->rdma_ctrl::register_memory 这里把全部空间都登记了
//登记一个memory 
        bool register_memory(uint64_t mr_id, const char *buf, uint64_t size,
                             RNicHandler *rnic, int flag) {
            Memory *m = new Memory(buf, size, rnic->pd, flag); // 这里登记mr 最好重新申请一块pm mmap到操作系统对应的空间
            if (!m->valid()) {
                NOVA_LOG(WARNING) << "register mr to rnic error: "
                                  << strerror(errno);
                delete m;
                return false;
            }
            {
                SCS s;
                if (mrs_.find(mr_id) != mrs_.end()) { // 这里限制了自己这个node只能register 1个mr 应该改成允许多个
                    NOVA_LOG(WARNING) << "mr " << mr_id
                                      << " has already been registered!";
                    delete m;
                } else {
                    mrs_.insert(std::make_pair(mr_id, m));
                }
            };
            return true;
        }

        int get_default_mr(MemoryAttr &attr) {
            SCS s;
            for (auto it = mrs_.begin(); it != mrs_.end(); ++it) {
                int idx = it->first;
                attr = it->second->rattr;
                return idx;
            }
            return -1;
        }

// 这里一个 改了可能会变成多个
//获得本地的mr的信息
        MemoryAttr get_local_mr(uint64_t mr_id) {
            MemoryAttr attr = {};
            {
                SCS s;
                if (mrs_.find(mr_id) != mrs_.end())
                    attr = mrs_[mr_id]->rattr;
            }
            return attr;
        }

        void clear_dev_info() {
            cached_infos_.clear();
        }

        static std::vector<RNicInfo> query_devs_helper() {
            int num_devices = 0;
            struct ibv_device **dev_list = nullptr;
            std::vector<RNicInfo> res;

            { // query the device and its active ports using the underlying APIs
                dev_list = ibv_get_device_list(&num_devices);
                int temp_devices = num_devices;

                if (dev_list == nullptr) {
                    NOVA_LOG(ERROR) << "cannot get ib devices.";
                    num_devices = 0;
                    goto QUERY_END;
                }

                for (uint dev_id = 0; dev_id < temp_devices; ++dev_id) {

                    struct ibv_context *ib_ctx = ibv_open_device(
                            dev_list[dev_id]);
                    if (ib_ctx == nullptr) {
                        NOVA_LOG(ERROR) << "open dev " << dev_id << " error: "
                                        << strerror(errno) << " ignored";
                        num_devices -= 1;
                        continue;
                    }
                    res.emplace_back(ibv_get_device_name(ib_ctx->device),
                                     dev_id, ib_ctx);
                    QUERY_DEV_END:
                    // close ib_ctx
                    RDMA_VERIFY(INFO, ibv_close_device(ib_ctx) == 0)
                        << "failed to close device " << dev_id;
                }
            }

            QUERY_END:
            if (dev_list != nullptr)
                ibv_free_device_list(dev_list);
            return res;
        }

        std::vector<RNicInfo> query_devs() {
            if (cached_infos_.size() != 0) {
                return cached_infos_;
            }
            cached_infos_ = query_devs_helper();
            return std::vector<RNicInfo>(cached_infos_.begin(),
                                         cached_infos_.end());
        }

        RNicHandler *get_device() {
            return rnic_instance();
        }

        void close_device() {
            if (rnic_instance() != nullptr) delete rnic_instance();
            rnic_instance() = nullptr;
        }

        void close_device(RNicHandler *rnic) {
            if (rnic != nullptr)
                delete rnic;
        }

//线程开启之后运行的函数
        static void *connection_handler_wrapper(void *context) {
            return ((RdmaCtrlImpl *) context)->connection_handler();
        }

//线程开启，用来管理连接 用于rdma之间互相的连接
        /**
         * Using TCP to connect in-coming QP & MR requests
         */
        void *connection_handler(void) {
            pthread_detach(pthread_self());
            auto listenfd = PreConnector::get_listen_socket(local_ip_,
                                                            tcp_base_port_);
            int opt = 1;
            RDMA_VERIFY(ERROR, setsockopt(listenfd, SOL_SOCKET,
                                          SO_REUSEADDR | SO_REUSEPORT, &opt,
                                          sizeof(int)) == 0)
                << "unable to configure socket status.";
            RDMA_VERIFY(ERROR, listen(listenfd, 24) == 0)
                << "TCP listen error: " << strerror(errno);
            while (running_) { // new RdmaCtrl后跑到这里卡住 等待后续
// rdma_handler::start->nova_rdmarc_broker::init->nova_rdmarc_broker::InitializeQPs::get_remote_mr
                asm volatile("":: : "memory");
                struct sockaddr_in cli_addr = {0};
                socklen_t clilen = sizeof(cli_addr);
//接收到一个新的端口的连接请求
                auto csfd = accept(listenfd, (struct sockaddr *) &cli_addr,
                                   &clilen);
                if (csfd < 0) {
                    NOVA_LOG(ERROR) << "accept a wrong connection error: "
                                    << strerror(errno);
                    continue;
                }
//等待对方发送信息
                if (!PreConnector::wait_recv(csfd, 10000)) {
                    close(csfd);
                    continue;
                }
//接受对方发送的信息
                ConnArg arg = {};
                auto n = recv(csfd, (char *) (&arg), sizeof(ConnArg),
                              MSG_WAITALL);
                if (n != sizeof(ConnArg)) {
                    // an invalid message
                    close(csfd);
                    continue;
                }
                ConnReply reply = {};
                reply.ack = ERR;
                { // in a global critical section
                    SCS s;
                    switch (arg.type) {
//对方的服务器已经终结
                        case ConnArg::TERMINATE:
                            terminate_mutex_.lock();
                            NOVA_LOG(INFO) << "Received terminate from "
                                           << arg.payload.node_id;
                            terminated_node_ids_.insert(arg.payload.node_id);
                            terminate_mutex_.unlock();
                            reply.ack = SUCC;
                            break;
//发过来的是mr的请求
                        case ConnArg::MR: // 首先对面请求本地的mr相关信息 将本方的mr信息返回
                            if (mrs_.find(arg.payload.mr.mr_id) != mrs_.end()) { // 这里只发送了1个 建立pm之后要发送第二个
                                memcpy((char *) (&(reply.payload.mr)),
                                       (char *) (&(mrs_[arg.payload.mr.mr_id]->rattr)),
                                       sizeof(MemoryAttr));
                                reply.ack = SUCC;
                            };
                            break;
//发过来的是qp的请求
                        case ConnArg::QP: { // 对面获得了本地的mr相关信息，然后就发送qp请求 返回本方的qp attr
                            qp_callback_(
                                    arg.payload.qp); // call the user callback
                            QP *qp = NULL;
                            switch (arg.payload.qp.qp_type) {
//UD类型的qp，
                                case IBV_QPT_UD: {
                                    UDQP *ud_qp = get_qp<UDQP, get_ud_key>(
                                            create_ud_idx(
                                                    arg.payload.qp.from_worker,
                                                    arg.payload.qp.from_index));
                                    if (ud_qp != nullptr && ud_qp->ready()) {
                                        qp = ud_qp;
                                        NOVA_LOG(INFO) << "Received request "
                                                       << arg.payload.qp
                                                               .from_worker
                                                       << ":" << arg.payload.qp
                                                               .from_index
                                                       << ":"
                                                       << qp->get_attr().qpn;
                                    }
                                }
                                    break;
//RC类型的qp
                                case IBV_QPT_RC: {
                                    RCQP *rc_qp = get_qp<RCQP, get_rc_key>(
                                            create_rc_idx(
                                                    arg.payload.qp.from_node,
                                                    arg.payload.qp.from_worker,
                                                    arg.payload.qp.from_index));
                                    qp = rc_qp;
                                }
                                    break;
                                default:
                                    NOVA_LOG(ERROR)
                                        << "unknown QP connection type: "
                                        << arg.payload.qp.qp_type;
                            }
                            if (qp != nullptr) {
                                reply.payload.qp = qp->get_attr();
                                reply.ack = SUCC;
                            }
                            reply.payload.qp.node_id = node_id_;
                            break;
                        }
                        default:
                            NOVA_LOG(WARNING)
                                << "received unknown connect type " << arg.type;
                    }
                } // end simple critical section protection
                PreConnector::send_to(csfd, (char *) (&reply),
                                      sizeof(ConnReply));
                PreConnector::wait_close(
                        csfd); // wait for the client to close the connection
            }
            // end of the server
            close(listenfd);
        }

    private:
        friend class RdmaCtrl;

        static RNicHandler *&rnic_instance() {
            static thread_local RNicHandler *handler = NULL;
            return handler;
        }

        std::vector<RNicInfo> cached_infos_;

        // registered MRs at this control manager
        std::map<uint64_t, Memory *> mrs_;

//key->qp的映射
        // created QPs on this control manager
        std::map<uint64_t, QP *> qps_;

        // local node information
//本地这个server的id
        const int node_id_;
//本地tcp的端口
        const int tcp_base_port_;
//本地的ip地址，其实就是localhost
        const std::string local_ip_;

//用于记录发送来已经结束信息的node id
        std::mutex terminate_mutex_;
        std::set<uint64_t> terminated_node_ids_;

//启动的线程的tid
        pthread_t handler_tid_;
        bool running_ = true;

//连接之后的回调函数，其实都置为空了
        // connection callback function
        connection_callback_t qp_callback_;

//        bool
//        link_symmetric_rcqps(const std::vector<std::string> &cluster, int l_mrid, uint64_t mr_id, int wid, int idx) {
//
//            std::vector<bool> ready_list(cluster.size(), false);
//            std::vector<MemoryAttr> mrs;
//
//            MemoryAttr local_mr = get_local_mr(l_mrid);
//
//            for (auto s : cluster) {
//                // get the target mr
//                retry:
//                MemoryAttr mr = {};
//                auto rc = QP::get_remote_mr(s, tcp_base_port_, mr_id, &mr);
//                if (rc != SUCC) {
//                    usleep(2000);
//                    goto retry;
//                }
//                mrs.push_back(mr);
//            }
//
//            RDMA_ASSERT(mrs.size() == cluster.size());
//
//            while (true) {
//                int connected = 0, i = 0;
//                for (auto s : cluster) {
//
//                    if (ready_list[i]) {
//                        i++;
//                        connected++;
//                        continue;
//                    }
//                    RCQP *qp = create_rc_qp(QPIdx{.node_id = i, .worker_id = wid, .index = idx},
//                                            get_device(), &local_mr, IBV_QPT_RC, NULL);
//                    RDMA_ASSERT(qp != nullptr);
//
//                    if (qp->connect(s, tcp_base_port_,
//                                    QPIdx{.node_id = node_id_, .worker_id = wid, .index = idx}) == SUCC) {
//                        ready_list[i] = true;
//                        connected++;
//                        qp->bind_remote_mr(mrs[i]);
//                    }
//                    i++;
//                }
//                if (connected == cluster.size())
//                    break;
//                else
//                    usleep(1000);
//            }
//            return true; // This example does not use error handling
//        }

//登记一个指定的callback

        void register_qp_callback(connection_callback_t callback) {
            qp_callback_ = callback;
        }

    }; //

//下面基本都是包装类

// link to the novalsm class
    inline __attribute__ ((always_inline))
    RdmaCtrl::RdmaCtrl(int node_id, int tcp_base_port,
                       connection_callback_t callback, std::string ip)
            : impl_(new RdmaCtrlImpl(node_id, tcp_base_port, callback, ip)) {
    }

    inline __attribute__ ((always_inline))
    RdmaCtrl::~RdmaCtrl() {
        impl_.reset();
    }

    inline __attribute__ ((always_inline))
    std::vector<RNicInfo> RdmaCtrl::query_devs() {
        return impl_->query_devs();
    }

    inline __attribute__ ((always_inline))
    void RdmaCtrl::clear_dev_info() {
        return impl_->clear_dev_info();
    }

    inline __attribute__ ((always_inline))
    RNicHandler *RdmaCtrl::get_device() {
        return impl_->get_device();
    }

    inline __attribute__ ((always_inline))
    RNicHandler *RdmaCtrl::open_thread_local_device(DevIdx idx) {
        return impl_->open_thread_local_device(idx);
    }

    inline __attribute__ ((always_inline))
    RNicHandler *RdmaCtrl::open_device(DevIdx idx) {
        return impl_->open_device(idx);
    }

    inline __attribute__ ((always_inline))
    void RdmaCtrl::close_device() {
        return impl_->close_device();
    }

    inline __attribute__ ((always_inline))
    void RdmaCtrl::close_device(RNicHandler *rnic) {
        return impl_->close_device(rnic);
    }

//参数都对的上，只是类定义的时候搞了个默认参数
    inline __attribute__ ((always_inline))
    bool RdmaCtrl::register_memory(uint64_t id, const char *buf, uint64_t size,
                                   RNicHandler *rnic, int flag) {
        return impl_->register_memory(id, buf, size, rnic, flag);
    }

    inline __attribute__ ((always_inline))
    MemoryAttr RdmaCtrl::get_local_mr(uint64_t mr_id) {
        return impl_->get_local_mr(mr_id);
    }

    inline __attribute__ ((always_inline))
    uint64_t RdmaCtrl::get_default_mr(MemoryAttr &attr) {
        return impl_->get_default_mr(attr);
    }

    inline __attribute__ ((always_inline))
    RCQP *RdmaCtrl::create_rc_qp(QPIdx idx, RNicHandler *dev, MemoryAttr *attr,
                                 ibv_cq *cq, ibv_cq *recv_cq) {
        return impl_->create_rc_qp(idx, dev, attr, IBV_QPT_RC, cq, recv_cq);
    }

    inline __attribute__ ((always_inline))
    void RdmaCtrl::destroy_rc_qp(QPIdx idx) {
        impl_->destroy_rc_qp(idx);
    }

    inline __attribute__ ((always_inline))
    ibv_cq *RdmaCtrl::create_cq(RNicHandler *dev, int cqe) {
        return ibv_create_cq(dev->ctx, cqe, nullptr, nullptr, 0);
    }

    inline __attribute__ ((always_inline))
    RCQP *RdmaCtrl::create_uc_qp(QPIdx idx, RNicHandler *dev, MemoryAttr *attr,
                                 ibv_cq *cq, ibv_cq *recv_cq) {
        return impl_->create_rc_qp(idx, dev, attr, IBV_QPT_UC, cq, recv_cq);
    }

    inline __attribute__ ((always_inline))
    UDQP *
    RdmaCtrl::create_ud_qp(QPIdx idx, RNicHandler *dev, MemoryAttr *attr) {
        return impl_->create_ud_qp(idx, dev, attr);
    }

    inline __attribute__ ((always_inline))
    RCQP *RdmaCtrl::get_rc_qp(QPIdx idx) {
        return impl_->get_rc_qp(idx);
    }

    inline __attribute__ ((always_inline))
    UDQP *RdmaCtrl::get_ud_qp(QPIdx idx) {
        return impl_->get_ud_qp(idx);
    }

    inline __attribute__ ((always_inline))
    int RdmaCtrl::current_node_id() {
        return impl_->node_id_;
    }

    inline __attribute__ ((always_inline))
    int RdmaCtrl::listening_port() {
        return impl_->tcp_base_port_;
    }

//    inline __attribute__ ((always_inline))
//    bool RdmaCtrl::link_symmetric_rcqps(const std::vector<std::string> &cluster,
//                                        int l_mrid, uint64_t mr_id, int wid, int idx) {
//        return impl_->link_symmetric_rcqps(cluster, l_mrid, mr_id, wid, idx);
//    }

    inline __attribute__ ((always_inline))
    std::vector<RNicInfo> RdmaCtrl::query_devs_helper() {
        return RdmaCtrlImpl::query_devs_helper();
    }

    inline __attribute__ ((always_inline))
    void RdmaCtrl::register_qp_callback(connection_callback_t callback) {
        impl_->register_qp_callback(callback);
    }

    inline __attribute__ ((always_inline))
    std::set<uint64_t> RdmaCtrl::terminated_node_ids() {
        std::lock_guard<std::mutex> lock(impl_->terminate_mutex_);
        return impl_->terminated_node_ids_;
    }

    inline __attribute__ ((always_inline))
    bool RdmaCtrl::broadcast_termination(uint64_t thread_id,
                                         std::vector<std::string> ip,
                                         std::vector<int> port,
                                         uint64_t my_node_id,
                                         uint64_t nthreads) {
        std::lock_guard<std::mutex> lock(terminate_mutex_);
        terminated_thread_ids_.insert(thread_id);
        if (terminated_thread_ids_.size() < nthreads) {
            return false;
        }

        if (termination_broadcasted_) {
            return true;
        }
        termination_broadcasted_ = true;
        for (int i = 0; i < ip.size(); i++) {
// first check whether QP is valid to connect
            ConnArg arg = {};
            ConnReply reply = {};
            arg.type = ConnArg::TERMINATE;
            arg.payload.node_id = my_node_id;
            NOVA_LOG(INFO) << "Send terminate message to " << ip[i] << ":"
                           << port[i];
            auto ret = QPImpl::get_remote_helper(&arg, &reply, ip[i], port[i]);
            NOVA_ASSERT(ret == SUCC);
        }
        return true;
    }

};
