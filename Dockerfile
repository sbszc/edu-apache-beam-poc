FROM adoptopenjdk/maven-openjdk11:latest AS build
ADD . /app
WORKDIR /app
USER root
RUN mvn clean package

FROM gcr.io/dataflow-templates-base/java11-template-launcher-base:latest

ARG WORKDIR=/template
ARG MAINCLASS=com.eqfx.latam.poc.Main
ARG JARFILE=poc-1.0.jar
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY --from=build /app/target/${JARFILE} /template/
RUN mkdir -p ${WORKDIR}/lib
COPY --from=build /app/target/lib/ ${WORKDIR}/lib/

ENV FLEX_TEMPLATE_JAVA_CLASSPATH=/template/${JARFILE}
ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS=${MAINCLASS}