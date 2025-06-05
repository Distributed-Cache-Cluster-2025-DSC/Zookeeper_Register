# ───────────────────────────────────────────────────
# 1단계: Gradle 환경에서 ShadowJar를 만들어 내는 builder 이미지
# ───────────────────────────────────────────────────
FROM gradle:8.5-jdk17 AS builder

WORKDIR /app

# 1) Gradle build 파일들 복사 (의존성 캐싱을 위해)
COPY build.gradle settings.gradle ./

# 2) 소스 코드 전체 복사
COPY src ./src

# 3) ShadowJar(=fat JAR) 생성
RUN gradle shadowJar --no-daemon --build-cache

# ───────────────────────────────────────────────────
# 2단계: 경량화된 JRE 이미지를 활용한 런타임 이미지
# ───────────────────────────────────────────────────
FROM openjdk:17-slim

WORKDIR /app

# 빌더 단계에서 생성된 모든-의존성 포함 Jar 파일 복사
COPY --from=builder /app/build/libs/*-all.jar app.jar

# 애플리케이션 실행 명령어
ENTRYPOINT ["java", "-jar", "/app/app.jar"]