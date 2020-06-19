# 简介
    只提供实体redis到实体redis(计划支持twemproxy,普通redis)的同步
    工具是由https://github.com/alibaba/RedisShake V1.6+全量解包工具为基础，翻译成rust语言.
    极限性能优于golang 版本的RedisShake
    当前为beta版本，如需要到生产环境，请利用从库验证后发布
## 欢迎issues
## 鸣谢
    https://github.com/alibaba/RedisShake
## tips:
    当发现tcp流量偏低 (0.x MB)
    echo 'net.ipv4.tcp_rmem=12375648 16500864 24751296'>> /etc/sysctl.conf
    echo 'net.ipv4.tcp_wmem=12375648 16500864 24751296'>> /etc/sysctl.conf
    echo 'net.core.rmem_max=24751296' >> /etc/sysctl.conf
    echo 'net.core.wmem_max=24751296' >> /etc/sysctl.conf
    sysctl -p
    即可调高tcp流量
