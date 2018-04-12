#!/bin/bash
git clone https://github.com/martindacos/prodigen-backend-services /opt/prodigen-backend
cd /opt/prodigen-backend
exec mongod&
exec gradle bootRun
