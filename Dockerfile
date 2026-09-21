FROM node:20-alpine

ENV NODE_ENV=production

WORKDIR /app

# 先装依赖，利用镜像层缓存（按锁文件可复现安装）
COPY package*.json ./
RUN npm ci --omit=dev

# 拷贝源码与静态资源
COPY server.js ./
COPY public ./public

EXPOSE 3000

# 容器级健康检查（服务内置 /healthz）
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD node -e "require('http').get('http://127.0.0.1:'+(process.env.PORT||3000)+'/healthz',r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"

CMD ["node", "server.js"]
