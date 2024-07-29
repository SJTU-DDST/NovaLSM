#ifndef STORAGE_LEVELDB_DB_HASHLIST_H_
#define STORAGE_LEVELDB_DB_HASHLIST_H_

#include <vector>
#include <atomic>
#include <cassert>
#include <cstdlib>
#include "common/nova_console_logging.h"
#include "common/nova_common.h"
#include "util/arena.h"
#include "util/random.h"
#include "common/city_hash.h"
#include "db/dbformat.h"
#include "util/coding.h"

namespace leveldb {
    class Arena;

    // 暂定2w个桶
    template<typename Key, class Comparator>
    class HashList {
    private:
        struct Node;

    public:
        explicit HashList(Comparator cmp, Arena *arena);

        ~HashList();

        void init();

        HashList(const HashList &) = delete;

        HashList &operator=(const HashList &) = delete;

// 不涉及遍历的都用这三个
// 先计算用户key的哈希值，然后再进行查找和删除

        // Insert key into the list.
        // REQUIRES: nothing that compares equal to key is currently in the list.
        void Insert(const Key &key, uint64_t hash); // 带着userkey的hash进来查找

        // 这个方法没调用过 所以无所谓了
        // Returns true iff an entry that compares equal to key is in the list.
        bool Contains(const Key &key, uint64_t hash) const; // 待定

        // 这个方法新加入，用于memtable.cc中的查询，查找是否还有其他点查询
        Key Get(const Key &key, uint64_t hash, bool* s, Slice user_key) const; // 之后再加入

        // insert和get只有memtable.cc里面会调用，
        int Compare(const Key& key1, const Key& key2) const;

        // 生成iter的时候就自动开始大排序?
        // 不 要flush才大排序 不然就不做事情
        class Iterator {
        public:
            // Initialize an iterator over the specified list.
            // The returned iterator is not valid.
            explicit Iterator(HashList *list);

            // Returns true iff the iterator is positioned at a valid node.
            bool Valid() const;

            // Returns the key at the current position.
            // REQUIRES: Valid()
            const Key& key() const; // 这里的key实际上永远是char*类型 所以不能返回局部变量

            // Advances to the next position.
            // REQUIRES: Valid()
            void Next();

            // Advances to the previous position.
            // REQUIRES: Valid()
            void Prev();

            // Advance to the first entry with a key >= target
            void Seek(const Key &target);

            // Position at the first entry in list.
            // Final state of iterator is Valid() iff list is not empty.
            void SeekToFirst();

            // Position at the last entry in list.
            // Final state of iterator is Valid() iff list is not empty.
            void SeekToLast();

        private:

            // 所有的node*都接过来然后排序
            void initandsort();

            HashList *list_;
            // 当前迭代器遍历到的节点
            int pos_; 
            // Node *node_;
            std::vector<Node*> sorted_; // 将节点指针排序后的 只用于遍历!!!!
            // int iter_level_;
            // uint32_t sampled_puts_;
            // Intentionally copyable
        };

        // 因为atomic类直接分配没有说法 所以可以改

        std::atomic<Node*>* hashlist_;

        // std::vector<std::atomic<Node*>> hashlist_;

    private:

        // 用哈希值比较作为插入的依据
        Node* NewNode(const Key& key, uint64_t hash);

        //两个key是否相等
        bool Equal(const Key &a, const Key &b) const { // 传入的是const char*
            return (compare_(a, b) == 0);
        }

        // 有趣的

        // 下面的都不会使用，仅仅作为参考
        // key是否在n后面 compare(n->key, key) < 0 就是key在n后面
        bool KeyIsAfterNode(const Key &key, Node *n) const; // 

        // 这个key后面的第一个key 找到n compare_(n->key, key) >= 0的
        Node* FindGreaterOrEqual(const Key&key) const;
        
        // 找到n compare(n->key, key) < 0
        Node* FindLessThan(const Key &key) const;

        // 找到最后一个
        Node* FindLast() const;

        Comparator compare_;        
        Arena *arena_;

        // vector<std::atomic<Node*>> hashlist_;

        // std::atomic_int_fast32_t nputs_per_level[kMaxHeight];
        Random rnd_;
    };

    template<typename Key, class Comparator>
    struct HashList<Key, Comparator>::Node{
        explicit Node(const Key &k) : key(k){ // 这个里面既有key又有value了

        }
        Key const key;
        
        Node* Next(){
            return next_.load(std::memory_order_acquire);
        }

        void SetNext(Node* x){
            next_.store(x, std::memory_order_release);
        }

        Node* NoBarrier_Next(){
            return next_.load(std::memory_order_relaxed);
        }

        void NoBarrier_SetNext(Node* x){
            next_.store(x, std::memory_order_relaxed);
        }
        std::atomic<Node*> next_;
    
        // Node* next_; // 这里似乎不需要原子变量了

    };

    template<typename Key, class Comparator>
    typename HashList<Key, Comparator>::Node *
    HashList<Key, Comparator>::NewNode(
            const Key &key, uint64_t hash) {
        char* node_memory = arena_->AllocateAligned(sizeof(Node));
        return new(node_memory) Node(key);
        // uint64_t key_offset = key - arena_->Buf();
        //new(arena_->Buf() + node_memory_offset) Node(key_offset); // 这里相当于写了offset进去
        //return reinterpret_cast<Node*>(arena_->Buf() + node_memory_offset);        
        // char *const node_memory = arena_->AllocateAligned(
        //         sizeof(Node) + sizeof(std::atomic<Node *>) * (height - 1));
        // return new(node_memory) Node(key);
    }


// 理论上有iterator参与的话就应该是compaction了 而非读取？
// 有可能是读取 所以需要将读取的都改为非iterator端
// 尽量使iterator只用于遍历/序列化    
    template<typename Key, class Comparator>
    inline HashList<Key, Comparator>::Iterator::Iterator(HashList *list) {
        list_ = list;
        // node_ = nullptr;
        pos_ = -1;
        initandsort(); // 排序
    }

    template<typename Key, class Comparator>
    inline void HashList<Key, Comparator>::Iterator::initandsort(){
        for(int i = 0; i < 20000; i++){
            Node* cur = list_->hashlist_[i].load(std::memory_order_relaxed); //这个存疑
            while(cur != nullptr){
                sorted_.push_back(cur);
                cur = cur->Next();
            }   
        }
        sort(sorted_.begin(), sorted_.end(), 
            [this](Node* &a, Node* &b){ // 这样就是从小到大
                return list_->Compare(a->key, b->key) < 0;
            }
        );
    }

    template<typename Key, class Comparator>
    inline bool HashList<Key, Comparator>::Iterator::Valid() const {
        return pos_ >= 0 && pos_ < sorted_.size(); 
    }    

    template<typename Key, class Comparator>
    inline const Key& HashList<Key, Comparator>::Iterator::key() const {
        assert(Valid());
        return sorted_[pos_]->key;
    }

    template<typename Key, class Comparator>
    inline void HashList<Key, Comparator>::Iterator::Next() {
        assert(Valid());
        // node_ = node_->Next(iter_level_);
        // uint64_t next_offset = node_->Next(iter_level_); // 取offset 然后重新做
        // node_ = reinterpret_cast<Node*>(list_->arena_->Buf() + next_offset);
        ++pos_;
    }    

// lessthan是 <
// greater是 >=

    template<typename Key, class Comparator>
    inline void HashList<Key, Comparator>::Iterator::Prev() {
        // Instead of using explicit "prev" links, we just search for the
        // last node that falls before key.
        assert(Valid());
        // node_ = list_->FindLessThan(node_->key);

        // while(pos_ >= 0 && pos_ < sorted_.size()){
        //     if(list_->Compare(sorted_[]))
        // }
        if(pos_ == 0 || pos_ == -1){
            pos_ = -1;
        }else{
            int low = 0, high = pos_;
            while(low < high){
                int mid = (low + high) / 2;
                if(list_->Compare(sorted_[mid]->key, sorted_[pos_]->key) < 0){
                    low = mid + 1;
                }else{
                    high = mid;
                }
            }
            if(low <= 0){
                pos_ = -1;
            }else{
                pos_ = low - 1;
            }
        }
    }    

    
    
// 大于等于就是排在后面

    // 这里进行二分查找 相当于lower bound 返回第一个大于等于给定Key的东西
    template<typename Key, class Comparator>
    inline void HashList<Key, Comparator>::Iterator::Seek(const Key &target) {
        int low = 0, high = sorted_.size(); // 左闭右开
        while(low < high){
            int mid = (low + high) / 2;
            // mid < target
            if(list_->Compare(sorted_[mid]->key, target) < 0){ // 真代表target在sorted之后 // 找到第一个在target之后的东西
                low = mid + 1;
            }else{
                // mid >= target;
                high = mid;
            }
        }
        if(low < 0 || low >= sorted_.size()){
            pos_ = -1;
        }else{
            pos_ = low;
        }
    }

    template<typename Key, class Comparator>
    inline void HashList<Key, Comparator>::Iterator::SeekToFirst() {
        if(sorted_.size() != 0){
            pos_ = 0;
        }else{
            pos_ = -1;
        }
    }

    template<typename Key, class Comparator>
    inline void HashList<Key, Comparator>::Iterator::SeekToLast() {
        if(sorted_.size() != 0){
            pos_ = sorted_.size() - 1;
        }else{
            pos_ = -1;
        }
    }

// key是否应该排在node之后
    template<typename Key, class Comparator>
    bool
    HashList<Key, Comparator>::KeyIsAfterNode(const Key &key, Node *n) const {
        // null n is considered infinite
        return (n != nullptr) && (compare_(n->key, key) < 0);
    }

    template<typename Key, class Comparator>
    typename HashList<Key, Comparator>::Node *
    HashList<Key, Comparator>::FindGreaterOrEqual(const Key &key) const {
        // 难道是从头找么??
        ;
    }

    template<typename Key, class Comparator>
    typename HashList<Key, Comparator>::Node *
    HashList<Key, Comparator>::FindLessThan(const Key &key) const {
        ;
    }

    template<typename Key, class Comparator>
    typename HashList<Key, Comparator>::Node *
    HashList<Key, Comparator>::FindLast() const {
        ;
    }


    template<typename Key, class Comparator>
    HashList<Key, Comparator>::HashList(Comparator cmp, Arena* arena)
        : compare_(cmp),
          arena_(arena),
          // hashlist_(20000, static_cast<Node*>(nullptr)),
          rnd_(0xdeadbeef){
        //   for(int i = 0; i < 20000; i++){
        //     hashlist_.push_back(nullptr);
        //   }
        char* tmp = new char[sizeof(std::atomic<Node*>) * 20000];
        memset(tmp, 0, sizeof(std::atomic<Node*>) * 20000);
        hashlist_ = reinterpret_cast<std::atomic<Node*>*>(tmp);
        // hashlist_ = std::vector<std::atomic<Node*>>(20000, nullptr);
    }

    template<typename Key, class Comparator>
    HashList<Key, Comparator>::~HashList(){
        char* tmp = reinterpret_cast<char*>(hashlist_);
        delete[] tmp;
    }


    template<typename Key, class Comparator>
    void HashList<Key, Comparator>::init(){
        ;
    }    

// 1. 把hash去掉，这样原来的很多接口不用改
// 2. 把查找的iter去掉，都改为Get接口
// 3. 确保iter中调用的接口每一个都是用来遍历序列化的 而非点查询

// 插入进来
    template<typename Key, class Comparator>
    void HashList<Key, Comparator>::Insert(const Key &key, uint64_t hash) {
        Node* x = NewNode(key, hash);
        x->NoBarrier_SetNext(hashlist_[hash % 20000].load(std::memory_order_relaxed));
        // 加入队列头部
        Node* cur_next = x->Next();
        while(!hashlist_[hash % 20000].compare_exchange_weak(cur_next, x)){
            x->NoBarrier_SetNext(cur_next);
        }
        // hashlist_[hash]
    }

// 这个可能没有调用
// 找到下一个节点 并且
    template<typename Key, class Comparator>
    bool HashList<Key, Comparator>::Contains(const Key &key, uint64_t hash) const {
        Node* x = hashlist_[hash % 20000].load(std::memory_order_relaxed);
        while(x != nullptr){
            if(Equal(key, x->key)){
                return true;
            }else{
                x = x->Next();
            }
        }
        return false;
    }

// 去看seek的方法
    template<typename Key, class Comparator>
    Key HashList<Key, Comparator>::Get(const Key &key, uint64_t hash, bool* s, Slice user_key) const {
        Node* x = hashlist_[hash % 20000].load(std::memory_order_relaxed);
        while(x != nullptr){
            // 这里改为只比较 userkey的部分
            uint32_t key_length;
            const char *key_ptr = GetVarint32Ptr(x->key, x->key + 5, &key_length);
            if(compare_.comparator.user_comparator()->Compare(Slice(key_ptr, key_length - 8), user_key) == 0){
                *s = true;
                return x->key;
            }else{
                x = x->Next();
            }



            // if(){
            //     // *s = true;
            //     // return x->key;
            //     x = x->Next();
            // }else{
            //     *s = true;
            //     return x->key;
            //     // x = x->next_;
            // }
        }
        *s = false;
        return nullptr;
        //return false;
    }
    
// key1是否等于key2或者应该排在key2后面
// less < greaterorequal >=
    template<typename Key, class Comparator>
    int HashList<Key, Comparator>::Compare(const Key& key1, const Key& key2) const {
        return compare_(key1, key2);
    }

}




#endif