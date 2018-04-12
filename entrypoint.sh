#!/bin/bash
git clone https://github.com/Yagouus/prodigen-backend /opt/sensimet
cd /opt/sensimet
exec mongod&
exec gradle bootRun
