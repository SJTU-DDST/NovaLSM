import pickle
import sys
import os
import math
import matplotlib.pyplot as plt
def print_in_format(exps, number_of_tab):
	if type(exps) == dict:
		for exp in exps:
			print("\t" * number_of_tab + str(exp))
			print_in_format(exps[exp], number_of_tab + 1)
	elif type (exps) == list:
		print("\t" * number_of_tab + str(exps))
	else:
		print("\t" * number_of_tab + str(exps))

def explore(exps):
    # print("**********************************************************")
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps.keys()) #'0.99,256,1,0,false,uniform,W100,1,1,10240,64,1'
    # print("**********************************************************")
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1'].keys()) # 'thpt', 'coll', 'net', 'cpu', 'disk', 'wait_time', 'nwait', 'stall_time', 'disk_space', 'num_l0_tables', 'total_memtable_size', 'written_memtable_size', 'memtable_size_reduction', 'total_disk_reads', 'total_disk_writes', 'disk_space_timeline', 'latencies', 'thpt_timeline', 'cpu_timeline', 'net_timeline', 'disk_timeline', 'rdma_timeline', 'hit_rate', 'total_log_records', 'peak_thpt'
    # print("**********************************************************")
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['thpt']) # 105809.16666666667 条形图 1
    # print("**********************************************************")
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['coll']) # 0 [IB]InGbps 条形图 2
    #                                                                       #   [IB]OutGbps
    #                                                                       # 1 [IB]InGbps
    #                                                                       #   [IB]OutGbps
    #                                                                       # 2 [IB]InGbps
    #                                                                       #   [IB]OutGbps
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['net']) # 0 [NET]TxGbps 条形图 3
    #                                                                      # 1 [NET]TxGbps
    #                                                                      # 2 [NET]TxGbps
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['cpu']) # 0 CPU 条形图 4
    #                                                                      #   CORE0
    #                                                                      #   CORE1
    #                                                                      #     ...
    #                                                                      #
    #                                                                      # 1 CPU
    #                                                                      #   ...
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['disk']) # 0 [DISK]Util 条形图 5
    #                                                                       # 1 [DISK]Util
    #                                                                       # 2 [DISK]Util
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['wait_time']) # 0.0 条形图 6
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")   
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['nwait']) # 0 条形图 7
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")    
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['stall_time']) # 0.0 条形图 8
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")      
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['disk_space']) # 11393 条形图 9
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['num_l0_tables']) # 87 条形图 10  
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['total_memtable_size']) # 138011006551 条形图 11
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['written_memtable_size']) # 129192463111 条形图 12
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")    
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['memtable_size_reduction']) # 6.389739239197008 条形图 13
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")   
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['total_disk_reads']) # 329360608063 条形图 14
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['total_disk_writes']) # 338757568794 条形图 15
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")    
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['latencies']) # read average 条形图 16
    #                                                                            #      p95
    #                                                                            #      p99
    #                                                                            # write average
    #                                                                            #       ...
    #                                                                            #       ...
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['disk_timeline'].keys())
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['disk_space_timeline']) # 0 [..] 折线图 17
                                                                                     # 1 [..]
                                                                                     # 2 [..]
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")  
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['thpt_timeline']) # [...] 折线图 18  
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['cpu_timeline'].keys())  
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['cpu_timeline']) # 0 [...] 折线图 19
    #                                                                               # 1 [...]
    #                                                                               # 2 [...]
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['net_timeline']) # 0 [...] 折线图 20
    #                                                                               # 1 [...]
    #                                                                               # 2 [...]
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['disk_timeline']) # 0 [...] 折线图 21
    #                                                                                # 1 [...]
    #                                                                                # 2 [...]
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['rdma_timeline']) # 0 [...] 折线图 22                                                                          
    #                                                                                # 1 [...]
    #                                                                                # 2 [...] 
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['hit_rate']) # 0 条形图 23
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['total_log_records']) # 0 条形图 24
    # print("**********************************************************")   
    # print("**********************************************************")
    # print("**********************************************************")                                                                                   
    # print(exps['0.99,256,1,0,false,uniform,W100,1,1,10240,64,1']['peak_thpt']) # 110840.0 条形图 25
    pass   
        
        
# 'disk_timeline'
# 'thpt_timeline'
# 'cpu_timeline'
# 'net_timeline'
# 'rdma_timeline'        
        
class Plotter(object):
    def __init__(self, exps, exp, exp_dir):
        self.UNIT_WIDTH  = 2.3
        self.UNIT_HEIGHT = 2.3
        self.PAPER_WIDTH = 7   # USENIX text block width
        self.UNIT = 1000000.0       
        
        self.exps = exps
        self.exp = exp
        self.exp_dir = exp_dir

        self.out_dir  = "{}/results-{}".format(self.exp_dir, self.exp)
        self.out_file = self.out_dir + "/out.gp"
        self.out = 0 
    
    def _get_pdf_name(self):
        pdf_name = self.out_file
        outs = self.out_file.split(".")
        if outs[-1] == "gp" or outs[-1] == "gnuplot":
            pdf_name = '.'.join(outs[0:-1]) + ".pdf"
        pdf_name = os.path.basename(pdf_name)
        return pdf_name
    
    def _plot_header(self):
        n_unit = len(self.exps[exp].keys())
        n_col = min(n_unit, int(self.PAPER_WIDTH / self.UNIT_WIDTH))
        n_row = math.ceil(float(n_unit) / float(n_col))
        # print("set term pdfcairo size %sin,%sin font \',10\'" %
        #       (self.UNIT_WIDTH * n_col, self.UNIT_HEIGHT * n_row),
        #       file=self.out)
        # print("set_out=\'set output \"`if test -z $OUT; then echo %s; else echo $OUT; fi`\"\'"
        #       % self._get_pdf_name(), file=self.out)
        # print("eval set_out", file=self.out)
        # print("set multiplot layout %s,%s" % (n_row, n_col), file=self.out)
    
    def plot_histogram(self):
        pass
    
    def plot_linechart(self):
        pass
    
    def plot_all(self):
        os.system("mkdir -p {}".format(self.out_dir))
        self.out = open(self.out_file, "w")
        
        
def set_plot_config():
    # plt.rcParams['font.sans-serif'] = 'SimHei'
    plt.rcParams['axes.unicode_minus'] = False
    # plt.rcParams['axes.facecolor'] = 'white'
        
# 5 * 5    
def simple_plot(exp_dir, exps, exp):
    results_dir = exp_dir + "/results-" + exp
    fig = plt.figure(figsize=(20, 20))
    
    index = 0
    for metric in exps[exp]:
        ax_cur = plt.subplot(5, 5, index+1)
        patch = ax_cur.patch
        patch.set
        if type(exps[exp][metric]) == dict:
            if "timeline" in metric:
                for i in exps[exp][metric]:
                    ax_cur.plot(range(len(exps[exp][metric][i])), exps[exp][metric][i], label = i)
                ax_cur.set_ylim(bottom=0, auto=True)
                ax_cur.set_title(metric)
                ax_cur.legend()
            elif metric == "latencies" :
                values = []
                labels = []
                for i in exps[exp][metric]:
                    for j in exps[exp][metric][i]:
                        values.append(exps[exp][metric][i][j])
                        labels.append(str(i) + "_" + str(j))
                        # ax_cur.plot(range(6), exps[exp][metric][i][j], label = str(i) + "_" + str(j))
                        # ax_cur.bar(range(5), [0, 0, exps[exp][metric], 0, 0], tick_label = ['n','n','cur','n','n'], width = 0.3)
                ax_cur.bar(range(len(values)), values, tick_label = labels, width = 0.3)
                for x,y in zip(range(len(values)), values):
                    if type(y) != float:
                        y = 0.0
                    ax_cur.text(x+0.05,y+0.05,'%.5f' %y, ha='center',va='bottom')
                ax_cur.set_ylim(bottom=0, auto=True)
                ax_cur.set_title(metric)
                ax_cur.legend()
            else:
                if "cpu" in metric:
                    values = []
                    labels = []
                    for i in exps[exp][metric]:
                        for j in exps[exp][metric][i]:
                            values.append(exps[exp][metric][i][j])
                            labels.append(str(i) + "_" + str(j))
                            # ax_cur.plot(range(6), exps[exp][metric][i][j], label = str(i) + "_" + str(j))
                            # ax_cur.bar(range(5), [0, 0, exps[exp][metric], 0, 0], tick_label = ['n','n','cur','n','n'], width = 0.3)
                    ax_cur.bar(range(len(values)), values, width = 0.3)
                    ax_cur.set_ylim(bottom=0, auto=True)
                    ax_cur.set_title(metric)
                    ax_cur.legend()
                else:
                    values = []
                    labels = []
                    for i in exps[exp][metric]:
                        for j in exps[exp][metric][i]:
                            values.append(exps[exp][metric][i][j])
                            labels.append(str(i) + "_" + str(j))
                            # ax_cur.plot(range(6), exps[exp][metric][i][j], label = str(i) + "_" + str(j))
                            # ax_cur.bar(range(5), [0, 0, exps[exp][metric], 0, 0], tick_label = ['n','n','cur','n','n'], width = 0.3)
                    ax_cur.bar(range(len(values)), values, tick_label = labels, width = 0.3)
                    for x,y in zip(range(len(values)), values):
                        ax_cur.text(x+0.05,y+0.05,'%.2f' %y, ha='center',va='bottom')
                    ax_cur.set_ylim(bottom=0, auto=True)
                    ax_cur.set_title(metric)
                    ax_cur.legend()
        elif type(exps[exp][metric]) == list:
            ax_cur.plot(range(len(exps[exp][metric])), exps[exp][metric])
            ax_cur.set_ylim(bottom=0, auto=True)
            ax_cur.set_title(metric)
        else:
            ax_cur.bar(range(5), [0, 0, exps[exp][metric], 0, 0], tick_label = ['n','n','cur','n','n'], width = 0.3)
            ax_cur.set_ylim(bottom=0, auto=True)
            ax_cur.set_title(metric)
            ax_cur.text(2+0.05,exps[exp][metric]+0.05,'%.2f' %exps[exp][metric], ha='center',va='bottom')
            # for x,y in zip(range(5),[0, 0, exps[exp][metric], 0, 0]):
            #    ax_cur.text(x+0.05,y+0.05,'%.2f' %y, ha='center',va='bottom')
            # ax_cur.legend()
        index = index + 1
        
        
        
    # plt.tight_layout()
    plt.show()
    plt.savefig(results_dir + '.png', # ⽂件名：png、jpg、pdf
                dpi = 100, # 保存图⽚像素密度
                # facecolor = 'violet', # 视图与边界之间颜⾊设置
                # edgecolor = 'lightgreen', # 视图边界颜⾊设置
                bbox_inches = 'tight')# 保存图⽚完整
        

if __name__ == "__main__":
    exp_dir = sys.argv[1]
    with open(exp_dir + "/results.pickle", 'rb') as f:
        exps = pickle.load(f)
        
    set_plot_config()
    # print_in_format(exps, 0)
    # explore(exps)
    for exp in exps:
        # print(len(exps[exp].keys()))
        simple_plot(exp_dir, exps, exp)