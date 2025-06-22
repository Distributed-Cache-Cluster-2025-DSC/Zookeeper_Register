# 1단계: Java 17 환경에서 Gradle을 사용하여 JAR 빌드
FROM gradle:8.5-jdk17 AS builder

WORKDIR /app

# Gradle 설정 파일들 먼저 복사 (의존성 캐싱 활용)
COPY build.gradle settings.gradle ./ 
# gradlew와 wrapper도 필요하다면 복사합니다. 여기서는 gradle 이미지를 직접 사용합니다.
# COPY gradlew ./ 
# COPY gradle ./gradle

# 의존성 먼저 다운로드 (소스 변경 없이 의존성만 변경되었을 때 이 레이어까지 캐시 활용)
# RUN ./gradlew build --no-daemon --build-cache || echo "No settings.gradle, skipping dependency download step"
# 위 명령어 대신, gradle 이미지에서는 바로 빌드해도 괜찮습니다.

# 소스 코드 복사
COPY src ./src

# Gradle 빌드를 실행하여 shadow JAR 생성 (fat JAR)
# gradlew가 있다면 ./gradlew shadowJar --no-daemon
RUN gradle shadowJar --no-daemon --build-cache

# 2단계: 작은 JRE 이미지를 기반으로 최종 이미지 생성
FROM openjdk:17

WORKDIR /app

# 빌드 단계에서 생성된 shadow JAR 파일 복사
# shadowJar 설정에 따라 파일명 패턴이 projectname-version-all.jar 형태가 됩니다.
COPY --from=builder /app/build/libs/*-all.jar app.jar

# 애플리케이션 실행 명령어
ENTRYPOINT ["java", "-jar", "/app/app.jar"] 