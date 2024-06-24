#ifndef STORAGE_LEVELDB_DB_PMSKIPLIST_H_
#define STORAGE_LEVELDB_DB_PMSKIPLIST_H_


#include <atomic>
#include <cassert>
#include <cstdlib>
#include "common/nova_console_logging.h"
#include "common/nova_common.h"

#include "ltc/db_helper.h" // 这个是ycsb comparator

#include "util/pmarena.h"
#include "util/random.h"

namespace leveldb {
    
    class PMArena;

// key是const char* comparator是ycsb comparator
// 这里面的key应该改为偏移 uint64_t!!!
// 这里面的key是offset，用于指示arena里面的位置，comparator应该有些区别了

// comparator是keycomparator类型 包装了internal key comparator
// 里面存了user comparator(ycsb comparator)

// 里面有两个方法 既可以比internalkey 也可以比 slice
// Compare比slice

    template<typename Key, class Comparator>
    // key const char* 还是当成const char *好
    // comparator PMMemTable::KeyComparator
    class PMSkipList {
    private:

        struct Node;

    public:
        // cmp的比较方式有所改变，需要变成索引相关的比较方式，这里换成其他
        explicit PMSkipList(Comparator cmp, PMArena* pmarena); // 这个调用的时候pmarena还没初始化
        // 加一个init函数

        void init();

        PMSkipList(const PMSkipList&) = delete;

        PMSkipList& operator=(const PMSkipList&) = delete;

        void Insert(const Key &key); // 内部的

        // 这个方法没有被调用过!!!!
        bool Contains(const Key &key) const; // 外部的

        Node* SpecialEnd(){ // 这个相当于nullptr 没人会开始指向这个!
            return reinterpret_cast<Node*>(pmarena_->Buf());
        }

        char* Begin(){
            return pmarena_->Buf();
        }

        // skiplist遍历所用
        class Iterator{
        public:
            explicit Iterator(const PMSkipList *list, uint32_t sampled_puts = 0);

            bool Valid() const;

            const Key &key() const;

            void Next();

            void Prev();

             // 只有这里是外来的key 也就是说 不在offset里面
            void Seek(const Key &target);

            void SeekToFirst();

            void SeekToLast();

        private:
            // 遍历所用 感觉不太有所谓
            const PMSkipList *list_;
            Node* node_; // 这里还是用了指针，因为这里不是存储
            int iter_level_;
            uint32_t sampled_puts_;
        };

    private:
        enum {
            kMaxHeight = 12
        };

        inline int GetMaxHeight() const {
            //(*reinterpret_cast<std::atomic<int>*>(pmarena_->Buf() + max_height_offset_)).load(std::memory_order_relaxed);
            return (*max_height_).load(std::memory_order_relaxed);
        }

        // 都换成偏移!!!!
        // 不换了
        // 就存取的时候用吧
        Node* NewNode(const Key& key, int height);

        int RandomHeight();
        
        // 这个有可能要变
        // 要改!!
        // 如果是const char* 或许不用改
        bool Equal(const Key &a, const Key &b) const {
            return (compare_(a, b) == 0);
        }

        bool KeyIsAfterNode(const Key &key, Node* n) const;

        // 下面有可能要变
        Node* FindGreaterOrEqual(const Key &key, Node **prev) const;

        Node* FindLessThan(const Key &key) const;

        Node* FindLast() const;
        //

        Comparator compare_; // 这个无需放在pmarena里面
        PMArena* pmarena_; // 管理连续内存

        Node* head_; // 这个应该是虚拟头节点，自然通过newnode分配 所以也在里面
                           // offset = 0 和 nullptr相同
                           // 也就是说node* 如果为 buf的话 那就是原来的nullptr

        std::atomic<int>* max_height_; // 需要放在连续内存中 看一下大小！
        // uint64_t max_height_offset_; // max_height的offset

        std::atomic_int_fast32_t nputs_per_level[kMaxHeight]; // 不需要放在连续内存中

        Random rnd_; // 不需要放在连续内存里面
    };

    // 放在连续内存里面 指针全部换成uint64_t
    template<typename Key, class Comparator>
    struct PMSkipList<Key, Comparator>::Node {
        explicit Node(const uint64_t &k) : key(k) {

        }
        const uint64_t key; // uint64_t

        uint64_t Next(int n){
            assert(n >= 0);
            return next_[n].load(std::memory_order_acquire);
        }

        void SetNext(int n, uint64_t x){
            assert(n >= 0);
            next_[n].store(x, std::memory_order_release);
        }

        uint64_t NoBarrier_Next(int n){
            assert(n >= 0);
            return next_[n].load(std::memory_order_relaxed);
        }

        void NoBarrier_SetNext(int n, uint64_t x){
            assert(n >= 0);
            next_[n].store(x, std::memory_order_relaxed);
        }

    private:
        std::atomic<uint64_t> next_[1]; // 变为存偏移 next中的元素是某个层下一个元素的偏移
    };


    // node 里面存指向key value的偏移
    // 内部的
    // 这里的key还是const char* nullptr变为buf
    template<typename Key, class Comparator>
    typename PMSkipList<Key, Comparator>::Node*
    PMSkipList<Key, Comparator>::NewNode(const Key& key, int height){
        uint64_t node_memory_offset = pmarena_->AllocateAligned(
            sizeof(Node) + sizeof(std::atomic<uint64_t>) * (height - 1)
        );
        uint64_t key_offset = key - pmarena_->Buf();
        new(pmarena_->Buf() + node_memory_offset) Node(key_offset); // 这里相当于写了offset进去
        return reinterpret_cast<Node*>(pmarena_->Buf() + node_memory_offset);
    }

    template<typename  Key, class Comparator>
    inline PMSkipList<Key, Comparator>::Iterator::Iterator(const PMSkipList* list, uint32_t sampled_puts){
        list_ = list;
        node_ = nullptr;
        sampled_puts_ = sampled_puts;
        iter_level_ = 0;

// sampled_puts是干嘛的？？？
        if(sampled_puts_ > 0){
            iter_level_ = kMaxHeight - 1;
            while(iter_level_ >= 0){
                uint32_t nputs = list_->nputs_per_level[iter_level_].load(
                    std::memory_order_relaxed);
                if(nputs > sampled_puts_){
                    break;
                }
                iter_level_ -= 1;
            }
            if(iter_level_ < 0){
                iter_level_ = 0;
            }
        }

    }

    // 保证了新加的
    template<typename Key, class Comparator>
    inline bool PMSkipList<Key, Comparator>::Iterator::Valid() const {
        return node_ != nullptr && node_ != reinterpret_cast<Node*>(list_->pmarena_->Buf());
    }

    template<typename Key, class Comparator>
    inline const Key &PMSkipList<Key, Comparator>::Iterator::key() const {
        assert(Valid());
        return list_->pmarena_->Buf() + node_->key; // offset变为指针
        // return node_->key;
    }

    template<typename Key, class Comparator>
    inline void PMSkipList<Key, Comparator>::Iterator::Next(){
        assert(Valid());
        uint64_t next_offset = node_->Next(iter_level_); // 取offset 然后重新做
        node_ = reinterpret_cast<Node*>(list_->pmarena_->Buf() + next_offset);
    }

    template<typename Key, class Comparator>
    inline void PMSkipList<Key, Comparator>::Iterator::Prev(){
        NOVA_ASSERT(sampled_puts_ == 0);
        assert(Valid());
        node_ = list_->FindLessThan(list_->pmarena_->Buf() + node_->key);
        // 保险
        if(node_ == list_->head_ || node_ == nullptr || node_ == reinterpret_cast<Node*>(list_->pmarena_->Buf())){
            node_ = nullptr;
        }
    }

    template<typename Key, class Comparator>
    inline void PMSkipList<Key, Comparator>::Iterator::Seek(const Key &target) {
        NOVA_ASSERT(sampled_puts_ == 0);
        node_ = list_->FindGreaterOrEqual(target, nullptr);
    }

    template<typename Key, class Comparator>
    inline void PMSkipList<Key, Comparator>::Iterator::SeekToFirst(){
        uint64_t next_offset = list_->head_->Next(iter_level_);
        node_ = reinterpret_cast<Node*>(list_->pmarena_->Buf() + next_offset);
        // 可能是个无效地址
    }

    template<typename Key, class Comparator>
    inline void PMSkipList<Key, Comparator>::Iterator::SeekToLast(){
        NOVA_ASSERT(sampled_puts_ == 0);
        node_ = list_->FindLast();
        if(node_ == list_->head_ || node_ == reinterpret_cast<Node*>(list_->pmarena_->Buf())){
            node_ = nullptr;
        }
    }

    template<typename Key, class Comparator>
    int PMSkipList<Key, Comparator>::RandomHeight(){
        static const unsigned int kBranching = 4;
        int height = 1;
        while(height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)){
            height++;
        }
        assert(height > 0);
        assert(height <= kMaxHeight);
        return height;
    }

// key比node大 在后面?? 先比key再比序列号?? 严格大于!
    template<typename Key, class Comparator>
    bool PMSkipList<Key, Comparator>::KeyIsAfterNode(const Key &key, Node *n) const {
        // null n is considered infinite
        return (n != reinterpret_cast<Node*>(pmarena_->Buf())) && (n != nullptr) && (compare_(pmarena_->Buf() + n->key, key) < 0);
        // 加了一层保险
    }

// prev用于记录下各级的前一个节点，换句话说就是在各级的哪个节点下展开寻找
    template<typename Key, class Comparator>
    typename PMSkipList<Key, Comparator>::Node *
    PMSkipList<Key, Comparator>::FindGreaterOrEqual(const Key &key,
                                                  Node **prev) const {
        Node *x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
            uint64_t next_offset = x->Next(level);
            Node* next = reinterpret_cast<Node*>(pmarena_->Buf() + next_offset);
            // Node *next = x->Next(level);
            if (KeyIsAfterNode(key, next)) { // 一直到找到一个节点next
                // Keep searching in this list
                x = next;
            } else {
                if (prev != nullptr) prev[level] = x; // 记录当前节点
                if (level == 0) { // 如果当前已经是最后一层 将下一个节点一同返回 key <= next
                    return next; 
                } else {
                    // Switch to next list
                    level--; // 下面还有就在下一层找
                }
            }
        }
    }

// 严格小于
    template<typename Key, class Comparator>
    typename PMSkipList<Key, Comparator>::Node *
    PMSkipList<Key, Comparator>::FindLessThan(const Key &key) const {
        Node *x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
            assert(x == head_ || compare_(pmarena_->Buf() + x->key, key) < 0);
            uint64_t next_offset = x->Next(level);
            Node* next = reinterpret_cast<Node*>(pmarena_->Buf() + next_offset);
            // Node *next = x->Next(level);
            if (next == nullptr || next == reinterpret_cast<Node*>(pmarena_->Buf()) || compare_(pmarena_->Buf() + next->key, key) >= 0) { // next的比要找的大了
                if (level == 0) {
                    return x;
                } else {
                    // Switch to next list
                    level--;
                }
            } else {
                x = next;
            }
        }
    }

// 找到最后一个节点
    template<typename Key, class Comparator>
    typename PMSkipList<Key, Comparator>::Node *
    PMSkipList<Key, Comparator>::FindLast()
    const {
        Node *x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
            uint64_t next_offset = x->Next(level);
            Node* next = reinterpret_cast<Node*>(pmarena_->Buf() + next_offset);
           // Node *next = x->Next(level);
            if (next == nullptr || next == reinterpret_cast<Node*>(pmarena_->Buf())) {
                if (level == 0) {
                    return x;
                } else {
                    // Switch to next list
                    level--;
                }
            } else {
                x = next;
            }
        }
    }

    // template<typename Key, class Comparator>
    // PMSkipList<Key, Comparator>::PMSkipList(){

    // }

// 头节点是个dummy节点
    template<typename Key, class Comparator>
    PMSkipList<Key, Comparator>::PMSkipList(Comparator cmp, PMArena *pmarena)
            : compare_(cmp),
              pmarena_(pmarena),
              head_(nullptr),
              max_height_(nullptr),
              rnd_(0xdeadbeef) {
        // uint64_t max_height_offset = pmarena_->Allocate(sizeof(std::atomic<int>));
        // max_height_ = reinterpret_cast<std::atomic<int>*>(pmarena_->Buf() + max_height_offset);
        // *max_height_ = 1;
        // head_ = NewNode(pmarena_->Buf(), kMaxHeight); // 指向开头的 就是head_c key是0
        // for (int i = 0; i < kMaxHeight; i++) {
        //     head_->SetNext(i, 0); // 都是0
        //     nputs_per_level[i] = 0;
        // }
    }

    template<typename Key, class Comparator>
    void PMSkipList<Key, Comparator>::init(){
        uint64_t max_height_offset = pmarena_->Allocate(sizeof(std::atomic<int>));
        max_height_ = reinterpret_cast<std::atomic<int>*>(pmarena_->Buf() + max_height_offset);
        *max_height_ = 1;
        head_ = NewNode(pmarena_->Buf(), kMaxHeight); // 指向开头的 就是head_c key是0
        for (int i = 0; i < kMaxHeight; i++) {
            head_->SetNext(i, 0); // 都是0
            nputs_per_level[i] = 0;
        }
    }

    // 来自内部 放心使用
    template<typename Key, class Comparator>
    void PMSkipList<Key, Comparator>::Insert(const Key &key) {
        // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
        // here since Insert() is externally synchronized.
        Node *prev[kMaxHeight];
        Node *x = FindGreaterOrEqual(key, prev); // 找到路线和插入位置的下一个节点x

        // Our data structure does not allow duplicate insertion
        assert(x == nullptr || x == reinterpret_cast<Node*>(pmarena_->Buf()) || !Equal(key, pmarena_->Buf() + x->key));

        int height = RandomHeight();
        if (height > GetMaxHeight()) { // 开始的时候max_height_设置为1 初始化的时候
            for (int i = GetMaxHeight(); i < height; i++) {
                prev[i] = head_;
            }
            // It is ok to mutate max_height_ without any synchronization
            // with concurrent readers.  A concurrent reader that observes
            // the new value of max_height_ will see either the old value of
            // new level pointers from head_ (nullptr), or a new value set in
            // the loop below.  In the former case the reader will
            // immediately drop to the next level since nullptr sorts after all
            // keys.  In the latter case the reader will use the new node.
            (*max_height_).store(height, std::memory_order_relaxed);
        }

        x = NewNode(key, height); // 新申请节点和层数
        for (int i = 0; i < height; i++) {
            // NoBarrier_SetNext() suffices since we will add a barrier when
            // we publish a pointer to "x" in prev[i].
            x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i)); // 层数越高越靠上
            prev[i]->SetNext(i, static_cast<uint64_t>(reinterpret_cast<char*>(x) - pmarena_->Buf()));
            nputs_per_level[i].fetch_add(1, std::memory_order_relaxed);
        }
    }

    template<typename Key, class Comparator>
    bool PMSkipList<Key, Comparator>::Contains(const Key &key) const {
        Node *x = FindGreaterOrEqual(key, nullptr);
        if (x != nullptr && x != reinterpret_cast<Node*>(pmarena_->Buf()) && Equal(key, x->key)) {
            return true;
        } else {
            return false;
        }
    }    


}

#endif