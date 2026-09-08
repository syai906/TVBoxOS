package com.github.tvbox.osc.discovery;

import android.os.Handler;
import android.os.Looper;

import com.github.tvbox.osc.util.LOG;

import org.json.JSONObject;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

/**
 * TVAgent 局域网服务发现（方案 C）
 *
 * TVAgent 服务端以 UDP 广播方式周期性发送信标（beacon），本组件后台监听信标，
 * 解析出直播订阅地址（config_url）供 ApiConfig 自动填充，实现零配置接入与
 * 服务端 IP 变更的自愈切换。
 *
 * 信标格式（JSON over UDP，端口 5181）：
 * {"proto":"tvagent","v":1,"name":"TVAgent","config_url":"http://192.168.50.2:5180/m3u"}
 */
public class LanServiceDiscovery {

    public static final int BEACON_PORT = 5181;
    private static final String TAG = "LanServiceDiscovery";

    private static volatile LanServiceDiscovery instance;

    private final Object lock = new Object();
    private volatile String configUrl = "";
    private Thread listenerThread;
    private final Handler mainHandler = new Handler(Looper.getMainLooper());

    private LanServiceDiscovery() {
    }

    public static LanServiceDiscovery get() {
        if (instance == null) {
            synchronized (LanServiceDiscovery.class) {
                if (instance == null) {
                    instance = new LanServiceDiscovery();
                }
            }
        }
        return instance;
    }

    /** 应用启动时调用，拉起后台监听线程（幂等） */
    public synchronized void start() {
        if (listenerThread != null && listenerThread.isAlive()) return;
        listenerThread = new Thread(this::listenLoop, "tvagent-discovery");
        listenerThread.setDaemon(true);
        listenerThread.start();
        LOG.i(TAG + " started, listening on UDP " + BEACON_PORT);
    }

    private void listenLoop() {
        while (true) {
            DatagramSocket socket = null;
            try {
                socket = new DatagramSocket(BEACON_PORT);
                byte[] buf = new byte[2048];
                while (true) {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    socket.receive(packet);
                    handlePacket(packet);
                }
            } catch (Throwable t) {
                LOG.e(TAG + " listener error: " + t.getMessage());
            } finally {
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (Throwable ignored) {
                    }
                }
            }
            // 异常退出后稍作延迟再重建监听，避免热循环
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void handlePacket(DatagramPacket packet) {
        try {
            String msg = new String(packet.getData(), packet.getOffset(), packet.getLength(), "UTF-8");
            JSONObject json = new JSONObject(msg);
            if (!"tvagent".equals(json.optString("proto"))) return;
            String url = json.optString("config_url", "").trim();
            if (url.isEmpty()) return;
            boolean changed = false;
            synchronized (lock) {
                if (!url.equals(configUrl)) {
                    configUrl = url;
                    changed = true;
                }
            }
            if (changed) {
                LOG.i(TAG + " discovered TVAgent service: " + url
                        + " from " + packet.getAddress().getHostAddress());
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        } catch (Throwable ignored) {
            // 非信标报文（其他应用的 UDP 广播）直接忽略
        }
    }

    /** 当前已发现的配置地址，未发现为空串 */
    public String getConfigUrl() {
        return configUrl;
    }

    /**
     * 阻塞等待发现结果。已有结果立即返回；否则最多等待 timeoutMs 毫秒。
     * 须在非 UI 线程调用（ApiConfig 的配置加载均在后台线程）。
     */
    public String awaitConfigUrl(long timeoutMs) {
        if (!configUrl.isEmpty()) return configUrl;
        long deadline = System.currentTimeMillis() + timeoutMs;
        synchronized (lock) {
            while (configUrl.isEmpty()) {
                long remain = deadline - System.currentTimeMillis();
                if (remain <= 0) break;
                try {
                    lock.wait(Math.min(remain, 1000));
                } catch (InterruptedException e) {
                    break;
                }
                // 等待期间确认监听线程存活，挂了则重启
                start();
            }
        }
        return configUrl;
    }

    public interface DiscoveryCallback {
        void onDiscovered(String url);
    }

    /**
     * 后台等待发现结果，回调固定在主线程执行（url 为空表示超时未发现）。
     * UI 线程需要等待发现时务必用本方法，不要直接 awaitConfigUrl 造成卡顿。
     */
    public void awaitConfigUrlAsync(final long timeoutMs, final DiscoveryCallback callback) {
        final String cached = configUrl;
        if (!cached.isEmpty()) {
            mainHandler.post(new Runnable() {
                @Override
                public void run() {
                    callback.onDiscovered(cached);
                }
            });
            return;
        }
        Thread waiter = new Thread(new Runnable() {
            @Override
            public void run() {
                final String url = awaitConfigUrl(timeoutMs);
                mainHandler.post(new Runnable() {
                    @Override
                    public void run() {
                        callback.onDiscovered(url);
                    }
                });
            }
        }, "tvagent-discovery-await");
        waiter.setDaemon(true);
        waiter.start();
    }
}
